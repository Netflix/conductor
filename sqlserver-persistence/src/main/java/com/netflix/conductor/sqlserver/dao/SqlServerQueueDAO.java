package com.netflix.conductor.sqlserver.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.exception.ApplicationException;
import com.netflix.conductor.core.exception.ApplicationException.Code;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.sqlserver.config.SqlServerProperties;
import com.netflix.conductor.sqlserver.config.SqlServerProperties.QUEUE_STRATEGY;
import com.netflix.conductor.sqlserver.util.ExecuteFunction;
import com.netflix.conductor.sqlserver.util.Query;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SqlServerQueueDAO extends SqlServerBaseDAO implements QueueDAO {
    private static final Long UNACK_SCHEDULE_MS = 60_000L;
    private SqlServerProperties.QUEUE_STRATEGY queueStrategy;
    private String zone;

    public SqlServerQueueDAO(ObjectMapper om, DataSource ds, SqlServerProperties properties) throws SQLException{
        super(om, ds);
        long removeInterval = properties.getProcessAllRemovesInterval().toSeconds();
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(this::processAllUnacks,
                        UNACK_SCHEDULE_MS, UNACK_SCHEDULE_MS, TimeUnit.MILLISECONDS);
        if(removeInterval > 0)
            Executors.newSingleThreadScheduledExecutor()
                    .scheduleAtFixedRate(this::processAllRemoves,
                            removeInterval, removeInterval, TimeUnit.SECONDS);
            
        logger.debug(SqlServerQueueDAO.class.getName() + " is ready to serve");
        zone = properties.getZone();
        queueStrategy = properties.getQueueShardingStrategyAsEnum();
        if(queueStrategy == QUEUE_STRATEGY.LOCAL_ONLY) {
            if(zone.length() > 4)
                throw new ApplicationException(Code.BACKEND_ERROR, "zone must be at most 4 characters");
            if(zone == "GLBL") 
                logger.warn("!!!You've set the queue strategy to localOnly but you the zone is GLBL (Global)!!!");
            zone = properties.getZone();
        }
        else if(zone != "GLBL")
            logger.warn("If zone is GLBL than the shard strategy should be shared.");
    }

    @Override
    public void push(String queueName, String messageId, long offsetTimeInSecond) {
        logger.trace("queue push "+queueName);
        push(queueName, messageId, 0, offsetTimeInSecond);
    }

    @Override
    public void push(String queueName, String messageId, int priority, long offsetTimeInSecond) {
        logger.trace("queue push "+queueName);
        withTransaction(tx -> pushMessage(tx, queueName, messageId, null, priority, offsetTimeInSecond));
    }

    @Override
    public void push(String queueName, List<Message> messages) {
        logger.trace("queue push "+queueName);
        withTransaction(tx -> messages
                .forEach(message -> pushMessage(tx, queueName, message.getId(), message.getPayload(), message.getPriority(), 0)));
    }

    @Override
    public boolean pushIfNotExists(String queueName, String messageId, long offsetTimeInSecond) {
        return pushIfNotExists(queueName, messageId, 0, offsetTimeInSecond);
    }

    @Override
    public boolean pushIfNotExists(String queueName, String messageId, int priority, long offsetTimeInSecond) {
        logger.trace("queue push "+queueName);
        // Unique index ignores duplicate keys
        String PUSH_MESSAGE = String.join("\n",
            "declare @q_shard char(4) = ?",
            "declare @q_name varchar(255) = ?",
            "declare @m_id uniqueidentifier = ?",
            "",
            "",
            "if exists ( select 1 from [data].[queue_message] where queue_shard = @q_shard and queue_name = @q_name and message_id=@m_id )",
            "begin",
            "    delete from data.queue_removed where queue_shard = @q_shard and queue_name = @q_name and message_id=@m_id ",
            "",
            "    update [data].[queue_message]",
            "    set popped = 0",
            "    where queue_shard = @q_shard and queue_name = @q_name and message_id=@m_id ",
            "end  ",
            "else",
            "begin",
            "    insert into [data].[queue_message]( deliver_on, queue_shard, queue_name, message_id, priority, offset_time_seconds)",
            "    values ( DATEADD(second,?,SYSDATETIME()), @q_shard , @q_name , CONVERT(UNIQUEIDENTIFIER, @m_id),? ,? )",
            "end"
        );
        return getWithRetriedTransactions(tx -> {
            createQueueIfNotExists(tx, queueName);
            return executeWithReturn(tx, PUSH_MESSAGE, q -> q.addParameter(zone)
            .addParameter(queueName).addParameter(messageId).addParameter(offsetTimeInSecond)
            .addParameter(priority).addParameter(offsetTimeInSecond).executeUpdate() > 0); 
        });
    }

    @Override
    public List<String> pop(String queueName, int count, int timeout) {
        logger.trace("queue pop "+queueName+" count "+count);
        try {
            List<Message> messages = getWithRetriedTransactions(tx -> popMessages(tx, queueName, count, timeout));
            return messages.stream().map(Message::getId).collect(Collectors.toList());
        } catch (ApplicationException e) {
            logger.warn("Error while popping "+queueName+" "+count+" items",e);
            return new ArrayList<>();
        }
    }

    @Override
    public List<Message> pollMessages(String queueName, int count, int timeout) {
        logger.trace("queue pop "+queueName+" count "+count);
        try {
            return getWithRetriedTransactions(tx -> popMessages(tx, queueName, count, timeout));       
        } catch (ApplicationException e) {
            logger.warn("Error while polling "+queueName+" "+count+" items",e);
            return new ArrayList<>();
        }
    }

    @Override
    public void remove(String queueName, String messageId) {
        logger.trace("queue remove "+queueName);
        withTransaction(tx -> removeMessage(tx, queueName, messageId));
    }

    @Override
    public int getSize(String queueName) {
        logger.trace("queue size "+queueName);
        final String GET_QUEUE_SIZE = String.join("\n",
        "SELECT COUNT( qm.message_id ) ",
        "FROM [data].[queue_message] qm",
        "left outer join data.queue_removed qr",
        "on qm.queue_shard = qr.queue_shard and qm.queue_name = qr.queue_name and qm.message_id=qr.message_id",
        "WHERE qm.queue_name = ?",
        "and qr.queue_name is null"
        );
        return queryWithTransaction(GET_QUEUE_SIZE, q -> ((Long) q.addParameter(queueName).executeCount()).intValue());
    }

    @Override
    public boolean ack(String queueName, String messageId) {
        logger.trace("queue ack "+queueName+" id "+messageId);
        return getWithRetriedTransactions(tx -> removeMessage(tx, queueName, messageId));
    }

    @Override
    public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
        logger.trace("queue set unack timeout "+queueName+" id "+messageId);
        long updatedOffsetTimeInSecond = unackTimeout / 1000;

        final String UPDATE_UNACK_TIMEOUT = "UPDATE [data].[queue_message] WITH(RowLock) SET offset_time_seconds = ?, deliver_on = DATEADD(second, ?, SYSDATETIME()) WHERE queue_shard=? AND queue_name = ? AND message_id = CONVERT(UNIQUEIDENTIFIER, ?)";

        return queryWithTransaction(UPDATE_UNACK_TIMEOUT,
                q -> q.addParameter(updatedOffsetTimeInSecond).addParameter(updatedOffsetTimeInSecond)
                        .addParameter(zone).addParameter(queueName).addParameter(messageId).executeUpdate()) == 1;
    }

    @Override
    public void flush(String queueName) {
        logger.trace("queue flush "+queueName);
        final String FLUSH_QUEUE = "DELETE FROM [data].[queue_message] WITH(RowLock) WHERE queue_name = ?";
        executeWithTransaction(FLUSH_QUEUE, q -> q.addParameter(queueName).executeDelete());
    }

    @Override
    public Map<String, Long> queuesDetail() {
        logger.trace("queue details ");
        final String GET_QUEUES_DETAIL = String.join("\n",
            "SELECT q.queue_name, (",
            "SELECT count( w.message_id ) ",
            "    FROM [data].[queue_message] w WITH(NoLock) ",
            "    left outer join data.queue_removed qr",
            "    on w.queue_shard = qr.queue_shard and w.queue_name = qr.queue_name and w.message_id=qr.message_id",
            "    WHERE w.popped = 0 ",
            "    AND w.queue_name = q.queue_name",
            "    AND qr.queue_name is null",
            ") AS size ",
            "FROM [data].[queue] q WITH(NoLock)"
           );
        return queryWithTransaction(GET_QUEUES_DETAIL, q -> q.executeAndFetch(rs -> {
            Map<String, Long> detail = Maps.newHashMap();
            while (rs.next()) {
                String queueName = rs.getString("queue_name");
                Long size = rs.getLong("size");
                detail.put(queueName, size);
            }
            return detail;
        }));
    }

    @Override
    public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
        // @formatter:off
        final String GET_QUEUES_DETAIL_VERBOSE = "SELECT q.queue_name, \n" 
                + "       (SELECT count(*) as cc1 FROM [data].[queue_message] q1 WITH(NoLock) left outer join data.queue_removed qr WITH(NoLock) on q1.queue_shard=qr.queue_shard and q1.queue_name = qr.queue_name and q1.message_id=qr.message_id WHERE popped = 0 AND q1.queue_name = q.queue_name and qr.queue_name is null ) AS size,\n"
                + "       (SELECT count(*) as cc2 FROM [data].[queue_message] q2 WITH(NoLock) left outer join data.queue_removed qr WITH(NoLock) on q2.queue_shard=qr.queue_shard and q2.queue_name = qr.queue_name and q2.message_id=qr.message_id WHERE popped = 1 AND q2.queue_name = q.queue_name and qr.queue_name is null ) AS uacked \n"
                + "FROM [data].[queue] q WITH(NoLock)";
        // @formatter:on
        logger.trace("queue more details ");
        return queryWithTransaction(GET_QUEUES_DETAIL_VERBOSE, q -> q.executeAndFetch(rs -> {
            Map<String, Map<String, Map<String, Long>>> result = Maps.newHashMap();
            while (rs.next()) {
                String queueName = rs.getString("queue_name");
                Long size = rs.getLong("size");
                Long queueUnacked = rs.getLong("uacked");
                result.put(queueName, ImmutableMap.of("a", ImmutableMap.of( // sharding not implemented, returning only
                        // one shard with all the info
                        "size", size, "uacked", queueUnacked)));
            }
            return result;
        }));
    }

    @Override
    public boolean postpone(String queueName, String messageId, int priority, long postponeDurationInSeconds) {
        logger.trace("queue postpone "+queueName);
        final String QUEUE_POSTPONE = String.join("\n", 
            "UPDATE [data].[queue_message] WITH(RowLock)",
            "SET priority=?,deliver_on=DATEADD(second,?,deliver_on),offset_time_seconds=?,popped=0",
            "WHERE queue_shard=? AND queue_name=? AND message_id=CONVERT(UNIQUEIDENTIFIER, ?)"
        );
        executeWithTransaction(QUEUE_POSTPONE, q -> q.addParameter(priority)
        .addParameter(postponeDurationInSeconds).addParameter(Long.toString(postponeDurationInSeconds))
        .addParameter(zone).addParameter(queueName).addParameter(messageId)
        .executeUpdate());
        return true;
    }

    /**
     * Un-pop all un-acknowledged messages for all queues.

     * @since 1.11.6
     */
    public void processAllUnacks() {

        logger.trace("processAllUnacks started");

        final String PROCESS_ALL_UNACKS = String.join("\n",
            "UPDATE [data].[queue_message] WITH(RowLock)",
            "SET popped = 0 ",
            "from [data].[queue_message] qm",
            "left outer join data.queue_removed qr",
            "on qm.queue_shard = qr.queue_shard and qm.queue_name = qr.queue_name and qm.message_id=qr.message_id",
            "WHERE qr.queue_name is null",
            "and qm.queue_shard='%s' ",
            "AND qm.popped = 1 ",
            "AND DATEADD(second,-60,SYSDATETIME()) > qm.deliver_on"
        );
        executeWithTransaction(String.format(PROCESS_ALL_UNACKS, zone), Query::executeUpdate);
    }

    public void processAllRemoves() {

        logger.trace("processAllRemoves started");

        final String SQL = String.join("\n", 
            "delete qm",
            "output deleted.* into #tmpRemoved",
            "from data.queue_message qm",
            " inner join data.queue_removed qr",
            " on qm.queue_shard = qr.queue_shard and qm.queue_name = qr.queue_name and qm.message_id=qr.message_id",
            "delete qr",
            " from data.queue_removed qr",
            " inner join #tmpRemoved qm",
            " on qm.queue_shard = qr.queue_shard and qm.queue_name = qr.queue_name and qm.message_id=qr.message_id"
        );
        executeWithTransaction(String.format(SQL, zone), Query::executeUpdate);
    }

    @Override
    public void processUnacks(String queueName) {
        logger.trace("processAllUnacks started for "+queueName);
        final String PROCESS_UNACKS = String.join("\n",
            "UPDATE [data].[queue_message] WITH(RowLock)",
            "SET popped = 0 ",
            "from [data].[queue_message] qm",
            "left outer join [data].[queue_removed] qr",
            "on qm.queue_shard = qr.queue_shard and qm.queue_name = qr.queue_name and qm.message_id=qr.message_id",
            "WHERE qr.queue_name is null",
            "AND qm.queue_shard=? ",
            "AND qm.queue_name = ? ",
            "AND qm.popped = 1 ",
            "AND DATEADD(second,-60,SYSDATETIME())  > qm.deliver_on"
        );
        executeWithTransaction(PROCESS_UNACKS, q -> q.addParameter(zone).addParameter(queueName).executeUpdate());
    }

    @Override
    public boolean resetOffsetTime(String queueName, String messageId) {
        logger.trace("queue reset offset time for "+queueName);
        long offsetTimeInSecond = 0;    // Reset to 0
        final String SET_OFFSET_TIME = "UPDATE [data].[queue_message] WITH(RowLock) SET offset_time_seconds = ?, deliver_on = DATEADD(second,?,SYSDATETIME()) \n"
                + "WHERE queue_shard=? AND queue_name = ? AND message_id = CONVERT(UNIQUEIDENTIFIER, ?)";

        return queryWithTransaction(SET_OFFSET_TIME, q -> q.addParameter(offsetTimeInSecond)
                .addParameter(offsetTimeInSecond).addParameter(zone).addParameter(queueName).addParameter(messageId).executeUpdate() == 1);
    }

    private void pushMessage(Connection connection, String queueName, String messageId, String payload, Integer priority,
                             long offsetTimeInSecond) {

        createQueueIfNotExists(connection, queueName);
        String PUSH_MESSAGE = String.join("\n", 
            "DECLARE @p_queue_shard CHAR(4)=?",
            "DECLARE @p_queue_name VARCHAR(255)=?",
            "DECLARE @p_message_id UNIQUEIDENTIFIER=CONVERT(UNIQUEIDENTIFIER, ?)",
            "DECLARE @p_payload VARCHAR(4000)=?",
            "DECLARE @p_deliver_on DATETIME2=DATEADD(second,?,SYSDATETIME())",
            "IF EXISTS (SELECT 1 FROM [data].[queue_message] WHERE queue_shard=@p_queue_shard AND queue_name=@p_queue_name AND message_id=@p_message_id)",
            "UPDATE [data].[queue_message]",
            "SET payload=@p_payload, deliver_on=@p_deliver_on",
            "ELSE",
            "INSERT INTO [data].[queue_message](deliver_on, queue_shard, queue_name, message_id, priority, offset_time_seconds, payload)",
            "VALUES(@p_deliver_on, @p_queue_shard, @p_queue_name, @p_message_id, ?, ?, @p_payload)"
        );
        execute(connection, PUSH_MESSAGE, q -> {
            int a = q.addParameter(zone).addParameter(queueName)
            .addParameter(messageId).addParameter(payload).addParameter(offsetTimeInSecond)
            .addParameter(priority).addParameter(offsetTimeInSecond).executeUpdate();
        });
    }

    private boolean removeMessage(Connection connection, String queueName, String messageId) {
        final String REMOVE_MESSAGE = "INSERT INTO [data].[queue_removed](queue_shard,queue_name,message_id)VALUES (?,?,?)";
        try {
            return query(connection, REMOVE_MESSAGE,
                q -> q.addParameter(zone).addParameter(queueName).addParameter(messageId).executeDelete());
        } catch (ApplicationException e) {
            logger.warn("Potential error while removing message "+messageId+" from queue "+queueName, e);
            return true;
        }
    }

    /**
     * Atomically peek messages and update them to popped = 1
     * @param connection
     * @param queueName
     * @param count
     * @return
     */
    private List<Message> peekMessagesAndUpdate(Connection connection, String queueName, int count) {
        if (count < 1)
            return Collections.emptyList();


        final String PEEK_AND_UPDATE = String.join("\n", 
            "; with cte_qm as (",
            "   SELECT TOP %d id",
            "   FROM [data].[queue_message] qm WITH(xLock, RowLock)",
            "   left outer join data.queue_removed qr",
            "   on qm.queue_shard = qr.queue_shard and qm.queue_name = qr.queue_name and qm.message_id=qr.message_id ",
            "   WHERE qr.queue_name is null",
            "     AND qm.queue_shard='%s' ",
            "     AND qm.queue_name='%s' ",
            "     AND qm.popped=0 ",
            "     AND qm.deliver_on <= DATEADD(microsecond, 1000, SYSDATETIME())",
            "   ORDER BY priority DESC, deliver_on ASC, id ASC", // Using id instead of created_on is more reliable
            "   )",
            "update qm",
            "SET popped=1",
            "OUTPUT INSERTED.message_id, INSERTED.priority, INSERTED.payload",
            "FROM [data].[queue_message] qm",
            "INNER JOIN cte_qm c",
            "ON qm.id = c.id"
        );
        return query(connection, String.format(PEEK_AND_UPDATE,count,zone,queueName), 
                p -> p.executeAndFetch(rs -> {
                    List<Message> results = new ArrayList<>();
                    while (rs.next()) {
                        Message m = new Message();
                        m.setId(rs.getString("message_id").toLowerCase());
                        m.setPriority(rs.getInt("priority"));
                        m.setPayload(rs.getString("payload"));
                        results.add(m);
                    }
                    return results;
            }));
    }

    private List<Message> popMessages(Connection connection, String queueName, int count, int timeout) {
        long start = System.currentTimeMillis();
        List<Message> messages = peekMessagesAndUpdate(connection, queueName, count);
        

        while (messages.size() < count && ((System.currentTimeMillis() - start) < timeout)) {
            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
            messages.addAll(peekMessagesAndUpdate(connection, queueName, count));
        }

        return messages;
    }


    private void createQueueIfNotExists(Connection connection, String queueName) {
        logger.trace("Creating new queue '{}'", queueName);
        // Unique index ignores duplicate keys
        String CREATE_QUEUE = String.join("\n",
            "INSERT INTO [data].[queue] (queue_name)",
            "VALUES (?);"
        );
        execute(connection, CREATE_QUEUE, (ExecuteFunction)(q -> q.addParameter(queueName).executeUpdate()));
    }

    @Override
    public boolean containsMessage(String queueName, String messageId) {
        final String EXISTS_MESSAGE = String.join("\n", //
            "IF EXISTS (",
            "   SELECT qm.id ",
            "    FROM [data].[queue_message] qm",
            "    left outer join data.queue_removed qr",
            "    on qm.queue_shard = qr.queue_shard and qm.queue_name = qr.queue_name and qm.message_id=qr.message_id ",
            "    WHERE qr.queue_name is null",
            "    AND qm.queue_shard=? ",
            "    AND qm.queue_name = ? ",
            "    AND qm.message_id = CONVERT(UNIQUEIDENTIFIER, ?)",
            ")",
            "SELECT 1",
            "ELSE",
            "SELECT 0"
        );
        boolean exists = queryWithTransaction(EXISTS_MESSAGE, q -> q.addParameter(zone).addParameter(queueName).addParameter(messageId).exists());
        return exists;
    }
}
