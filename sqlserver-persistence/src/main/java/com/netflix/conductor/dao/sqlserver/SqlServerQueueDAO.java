package com.netflix.conductor.dao.sqlserver;

import com.amazonaws.services.kms.model.InvalidArnException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.sqlserver.SqlServerConfiguration;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Singleton
public class SqlServerQueueDAO extends SqlServerBaseDAO implements QueueDAO {
    private static final Long UNACK_SCHEDULE_MS = 60_000L;
    private SqlServerConfiguration.QUEUE_STRATEGY queueStrategy;
    private String instanceRack;

    @Inject
    public SqlServerQueueDAO(ObjectMapper om, DataSource ds, SqlServerConfiguration config) {
        super(om, ds);

        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(this::processAllUnacks,
                        UNACK_SCHEDULE_MS, UNACK_SCHEDULE_MS, TimeUnit.MILLISECONDS);
        logger.debug(SqlServerQueueDAO.class.getName() + " is ready to serve");
        queueStrategy = config.getQueueStrategy();
        instanceRack = config.getProperty("LOCAL_RACK", "");
    }

    @Override
    public void push(String queueName, String messageId, long offsetTimeInSecond) {
        logger.trace("queue push "+queueName);
        push(queueName, messageId, 0, offsetTimeInSecond);
    }

    @Override
    public void push(String queueName, String messageId, int priority, long offsetTimeInSecond) {
        String qn = calculateQueueName(queueName);
        logger.trace("queue push "+qn);
        withTransaction(tx -> pushMessage(tx, qn, messageId, null, priority, offsetTimeInSecond));
    }

    @Override
    public void push(String queueName, List<Message> messages) {
        String qn = calculateQueueName(queueName);
        logger.trace("queue push "+qn);
        withTransaction(tx -> messages
                .forEach(message -> pushMessage(tx, qn, message.getId(), message.getPayload(), message.getPriority(), 0)));
    }

    @Override
    public boolean pushIfNotExists(String queueName, String messageId, long offsetTimeInSecond) {
        return pushIfNotExists(queueName, messageId, 0, offsetTimeInSecond);
    }

    @Override
    public boolean pushIfNotExists(String queueName, String messageId, int priority, long offsetTimeInSecond) {
        String qn = calculateQueueName(queueName);
        logger.trace("queue push "+qn);
        return getWithRetriedTransactions(tx -> {
            if (!existsMessage(tx, qn, messageId)) {
                pushMessage(tx, qn, messageId, null, priority, offsetTimeInSecond);
                return true;
            }
            return false;
        });
    }

    @Override
    public List<String> pop(String queueName, int count, int timeout) {
        String qn = calculateQueueName(queueName);
        logger.trace("queue pop "+qn+" count "+count);
        List<Message> messages = getWithTransactionWithOutErrorPropagation(tx -> popMessages(tx, qn, count, timeout));
        if(messages == null) return new ArrayList<>();
        return messages.stream().map(Message::getId).collect(Collectors.toList());
    }

    @Override
    public List<Message> pollMessages(String queueName, int count, int timeout) {
        String qn = calculateQueueName(queueName);
        logger.trace("queue pop "+qn+" count "+count);
        List<Message> messages = getWithTransactionWithOutErrorPropagation(tx -> popMessages(tx, qn, count, timeout));
        if(messages == null) return new ArrayList<>();
        return messages;
    }

    @Override
    public void remove(String queueName, String messageId) {
        String qn = calculateQueueName(queueName);
        logger.trace("queue remove "+qn);
        withTransaction(tx -> removeMessage(tx, qn, messageId));
    }

    @Override
    public int getSize(String queueName) {
        String qn = calculateQueueName(queueName);
        logger.trace("queue size "+qn);
        final String GET_QUEUE_SIZE = "SELECT COUNT(*) FROM dbo.queue_message WHERE queue_name = ?";
        return queryWithTransaction(GET_QUEUE_SIZE, q -> ((Long) q.addParameter(qn).executeCount()).intValue());
    }

    @Override
    public boolean ack(String queueName, String messageId) {
        String qn = calculateQueueName(queueName);
        logger.trace("queue ack "+qn+" id "+messageId);
        return getWithRetriedTransactions(tx -> removeMessage(tx, qn, messageId));
    }

    @Override
    public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
        String qn = calculateQueueName(queueName);
        logger.trace("queue set unack timeout "+qn+" id "+messageId);
        long updatedOffsetTimeInSecond = unackTimeout / 1000;

        final String UPDATE_UNACK_TIMEOUT = "UPDATE dbo.queue_message SET offset_time_seconds = ?, deliver_on = DATEADD(second, ?, SYSDATETIME()) WHERE queue_name = ? AND message_id = ?";

        return queryWithTransaction(UPDATE_UNACK_TIMEOUT,
                q -> q.addParameter(updatedOffsetTimeInSecond).addParameter(updatedOffsetTimeInSecond)
                        .addParameter(qn).addParameter(messageId).executeUpdate()) == 1;
    }

    @Override
    public void flush(String queueName) {
        String qn = calculateQueueName(queueName);
        logger.trace("queue flush "+qn);
        final String FLUSH_QUEUE = "DELETE FROM dbo.queue_message WHERE queue_name = ?";
        executeWithTransaction(FLUSH_QUEUE, q -> q.addParameter(qn).executeDelete());
    }

    @Override
    public Map<String, Long> queuesDetail() {
        logger.trace("queue details ");
        final String GET_QUEUES_DETAIL = "SELECT q.queue_name, (SELECT count(*) FROM dbo.queue_message w WHERE w.popped = 0 AND w.queue_name = q.queue_name ) AS size FROM queue q";
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
                + "       (SELECT count(*) as cc1 FROM dbo.queue_message q1 WHERE popped = 0 AND q1.queue_name = q.queue_name) AS size,\n"
                + "       (SELECT count(*) as cc2 FROM dbo.queue_message q2 WHERE popped = 1 AND q2.queue_name = q.queue_name) AS uacked \n"
                + "FROM queue q";
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

    /**
     * Un-pop all un-acknowledged messages for all queues.

     * @since 1.11.6
     */
    public void processAllUnacks() {

        logger.trace("processAllUnacks started");

        final String PROCESS_ALL_UNACKS = "UPDATE dbo.queue_message SET popped = 0 WHERE popped = 1 AND DATEADD(second,-60,SYSDATETIME()) > deliver_on";
        executeWithTransaction(PROCESS_ALL_UNACKS, Query::executeUpdate);
    }

    @Override
    public void processUnacks(String queueName) {
        String qn = calculateQueueName(queueName);
        logger.trace("processAllUnacks started for "+qn);
        final String PROCESS_UNACKS = "UPDATE dbo.queue_message SET popped = 0 WHERE queue_name = ? AND popped = 1 AND DATEADD(second,-60,SYSDATETIME())  > deliver_on";
        executeWithTransaction(PROCESS_UNACKS, q -> q.addParameter(qn).executeUpdate());
    }

    @Override
    public boolean resetOffsetTime(String queueName, String messageId) {
        String qn = calculateQueueName(queueName);
        logger.trace("queue reset offset time for "+qn);
        long offsetTimeInSecond = 0;    // Reset to 0
        final String SET_OFFSET_TIME = "UPDATE dbo.queue_message SET offset_time_seconds = ?, deliver_on = DATEADD(second,?,SYSDATETIME()) \n"
                + "WHERE queue_name = ? AND message_id = ?";

        return queryWithTransaction(SET_OFFSET_TIME, q -> q.addParameter(offsetTimeInSecond)
                .addParameter(offsetTimeInSecond).addParameter(qn).addParameter(messageId).executeUpdate() == 1);
    }

    private boolean existsMessage(Connection connection, String queueName, String messageId) {
        final String EXISTS_MESSAGE = "SELECT COUNT(*) FROM dbo.queue_message WHERE queue_name = ? AND message_id = ?";
        return query(connection, EXISTS_MESSAGE, q -> q.addParameter(queueName).addParameter(messageId).exists());
    }

    private void pushMessage(Connection connection, String queueName, String messageId, String payload, Integer priority,
                             long offsetTimeInSecond) {

        createQueueIfNotExists(connection, queueName);
        String PUSH_MESSAGE = "MERGE [dbo].[queue_message] AS target " +
                              "USING (SELECT DATEADD(second,?,SYSDATETIME()) as col1, ? as col2, ? as col3,? as col4,? as col5,? as col6) AS source ( deliver_on, queue_name, message_id, priority, offset_time_seconds, payload) " + 
                              "ON source.queue_name = target.queue_name AND source.message_id = target.message_id " +
                              "WHEN MATCHED THEN " +
                              "UPDATE SET target.payload=source.payload, target.deliver_on=source.deliver_on " +
                              "WHEN NOT MATCHED THEN " +
                              "INSERT (deliver_on, queue_name, message_id, priority, offset_time_seconds, payload) " +
                              "VALUES (source.deliver_on, source.queue_name, source.message_id, source.priority, source.offset_time_seconds, source.payload);";
        execute(connection, PUSH_MESSAGE, q -> {
            int a = q.addParameter(offsetTimeInSecond).addParameter(queueName)
            .addParameter(messageId).addParameter(priority).addParameter(offsetTimeInSecond)
            .addParameter(payload).executeUpdate();
        });
    }

    private boolean removeMessage(Connection connection, String queueName, String messageId) {
        final String REMOVE_MESSAGE = "DELETE FROM dbo.queue_message WHERE queue_name = ? AND message_id = ?";
        return query(connection, REMOVE_MESSAGE,
                q -> q.addParameter(queueName).addParameter(messageId).executeDelete());
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
            "MERGE dbo.queue_message WITH(RowLock) target",
            "USING (",
            "   SELECT TOP %d id",
            "   FROM dbo.queue_message WITH(index(combo_queue_message), UpdLock, RowLock)",
            "   WHERE queue_name = '%s' AND popped = 0 AND deliver_on <= DATEADD(microsecond, 1000, SYSDATETIME())",
            "   ORDER BY priority DESC, deliver_on ASC, id ASC", // Using id instead of created_on is more reliable
            "   ) source",
            "ON target.id = source.id",
            "WHEN MATCHED THEN",
            "UPDATE SET popped=1",
            "OUTPUT INSERTED.message_id, INSERTED.priority, INSERTED.payload;"
        );
        List<Message> messages = query(connection, String.format(PEEK_AND_UPDATE, count, queueName), 
                p -> p.executeAndFetch(rs -> {
                    List<Message> results = new ArrayList<>();
                    while (rs.next()) {
                        Message m = new Message();
                        m.setId(rs.getString("message_id"));
                        m.setPriority(rs.getInt("priority"));
                        m.setPayload(rs.getString("payload"));
                        results.add(m);
                    }
                    return results;
                }));
        return messages;
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
        String CREATE_QUEUE = String.join("\n",
            "INSERT INTO [dbo].[queue] (queue_name)",
            "VALUES (?);"
        );
        execute(connection, CREATE_QUEUE, q -> q.addParameter(queueName).executeUpdate());
    }

    @Override
    public boolean containsMessage(String queueName, String messageId) {
        final String EXISTS_QUEUE = "SELECT COUNT(*) FROM dbo.queue_message WHERE queue_name = ? AND message_id = ?";
        boolean exists = queryWithTransaction(EXISTS_QUEUE, q -> q.addParameter(calculateQueueName(queueName)).addParameter(messageId).exists());
        return exists;
    }

    public String calculateQueueName(String queueName) {
        switch (queueStrategy) {
            case SHARED:
                return queueName;
            case LOCAL_ONLY:
                return String.format("%s.%s", queueName, instanceRack);
            default:
                throw new InvalidArnException("Invalid value for " + SqlServerConfiguration.QUEUE_STRATEGY_PROPERTY_NAME);
        }
    }
}
