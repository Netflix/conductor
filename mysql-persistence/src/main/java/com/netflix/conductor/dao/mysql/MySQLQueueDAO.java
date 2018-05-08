package com.netflix.conductor.dao.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.QueueDAO;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.sql.DataSource;
import org.apache.commons.lang.time.DateUtils;

public class MySQLQueueDAO extends MySQLBaseDAO implements QueueDAO {

    @Inject
    public MySQLQueueDAO(ObjectMapper om, DataSource ds) {
        super(om, ds);
    }

    @Override
    public void push(String queueName, String messageId, long offsetTimeInSecond) {
        withTransaction(tx -> pushMessage(tx, queueName, messageId, null, offsetTimeInSecond));
    }

    @Override
    public void push(String queueName, List<Message> messages) {
        withTransaction(tx ->
            messages.forEach(message ->
                pushMessage(tx, queueName, message.getId(), message.getPayload(), 0)
            )
        );
    }

    @Override
    public boolean pushIfNotExists(String queueName, String messageId, long offsetTimeInSecond) {
        return getWithTransaction(tx -> {
            if (!existsMessage(tx, queueName, messageId)) {
                pushMessage(tx, queueName, messageId, null, offsetTimeInSecond);
                return true;
            }
            return false;
        });
    }

    @Override
    public List<String> pop(String queueName, int count, int timeout) {
        long start = System.currentTimeMillis();
        List<String> foundsIds = peekMessages(queueName, count);

        while (foundsIds.size() < count && ((System.currentTimeMillis() - start) < timeout)) {
            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
            foundsIds = peekMessages(queueName, count);
        }

        ImmutableList<String> messageIds = ImmutableList.copyOf(foundsIds);
        return getWithTransaction(tx -> popMessages(tx, queueName, messageIds));
    }

    @Override
    public List<Message> pollMessages(String queueName, int count, int timeout) {
        final long start = System.currentTimeMillis();
        return getWithTransaction(tx -> {
            List<String> peekedMessageIds = peekMessages(tx, queueName, count);
            while(peekedMessageIds.size() < count && ((System.currentTimeMillis() - start) < timeout)) {
                Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
                peekedMessageIds = peekMessages(queueName, count);
            }

            final List<String> msgIds = ImmutableList.copyOf(peekedMessageIds);
            List<Message> messages = readMessages(tx, queueName, msgIds);
            popMessages(tx, queueName, msgIds);
            return ImmutableList.copyOf(messages);
        });
    }

    @Override
    public void remove(String queueName, String messageId) {
        withTransaction(tx -> removeMessage(tx, queueName, messageId));
    }

    @Override
    public int getSize(String queueName) {
        String GET_QUEUE_SIZE = "SELECT COUNT(*) FROM queue_message WHERE queue_name = ?";
        return queryWithTransaction(GET_QUEUE_SIZE, q -> ((Long) q.addParameter(queueName).executeCount()).intValue());
    }

    @Override
    public boolean ack(String queueName, String messageId) {
        return getWithTransaction(tx -> {
            if (existsMessage(tx, queueName, messageId)) {
                removeMessage(tx, queueName, messageId);
                return true;
            } else {
                return false;
            }
        });
    }

    @Override
    public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
        long updatedOffsetTimeInSecond = unackTimeout/1000;

        String UPDATE_UNACK_TIMEOUT =
            "UPDATE queue_message SET offset_time_seconds = ?, deliver_on = TIMESTAMPADD(SECOND, ?, created_on) WHERE queue_name = ? AND message_id = ?";

        return queryWithTransaction(UPDATE_UNACK_TIMEOUT, q ->
                q.addParameter(updatedOffsetTimeInSecond)
                        .addParameter(updatedOffsetTimeInSecond)
                        .addParameter(queueName)
                        .addParameter(messageId)
                        .executeUpdate()) == 1;
    }

    @Override
    public void flush(String queueName) {
        String FLUSH_QUEUE = "DELETE FROM queue_message WHERE queue_name = ?";
        executeWithTransaction(FLUSH_QUEUE, q -> q.addParameter(queueName).executeDelete());
    }

    @Override
    public Map<String, Long> queuesDetail() {

        String GET_QUEUES_DETAIL =
            "SELECT queue_name, (SELECT count(*) FROM queue_message WHERE popped = false AND queue_name = q.queue_name) AS size FROM queue q";

        return queryWithTransaction(GET_QUEUES_DETAIL, q -> q.executeAndFetch(rs -> {
            Map<String, Long> detail = Maps.newHashMap();
            while(rs.next()) {
                String queueName = rs.getString("queue_name");
                Long size = rs.getLong("size");
                detail.put(queueName, size);
            }
            return detail;
        }));
    }

    @Override
    public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
        //@formatter:off
        String GET_QUEUES_DETAIL_VERBOSE =
            "SELECT queue_name, \n" +
            "       (SELECT count(*) FROM queue_message WHERE popped = false AND queue_name = q.queue_name) AS size,\n" +
            "       (SELECT count(*) FROM queue_message WHERE popped = true AND queue_name = q.queue_name) AS uacked \n" +
            "FROM queue q";
        //@formatter:on

        return queryWithTransaction(GET_QUEUES_DETAIL_VERBOSE, q -> q.executeAndFetch(rs -> {
            Map<String, Map<String, Map<String, Long>>> result = Maps.newHashMap();
            while(rs.next()) {
                String queueName = rs.getString("queue_name");
                Long size = rs.getLong("size");
                Long queueUnacked = rs.getLong("uacked");
                result.put(queueName, ImmutableMap.of(
                        "a", ImmutableMap.of(   //sharding not implemented, returning only one shard with all the info
                                "size", size,
                                "uacked", queueUnacked
                        ))
                );
            }
            return result;
        }));
    }

    @Override
    public void processUnacks(String queueName) {
        String PROCESS_UNACKS = "UPDATE queue_message SET popped = false WHERE queue_name = ? AND popped = true AND CURRENT_TIMESTAMP > deliver_on";
        executeWithTransaction(PROCESS_UNACKS, q -> q.addParameter(queueName).executeUpdate());
    }

    @Override
    public boolean setOffsetTime(String queueName, String messageId, long offsetTimeInSecond) {
        String SET_OFFSET_TIME =
                "UPDATE queue_message SET offset_time_seconds = ?, deliver_on = TIMESTAMPADD(SECOND,?,created_on) \n" +
                        "WHERE queue_name = ? AND message_id = ?";

        return queryWithTransaction(SET_OFFSET_TIME, q ->
                q.addParameter(offsetTimeInSecond)
                        .addParameter(offsetTimeInSecond)
                        .addParameter(queueName)
                        .addParameter(messageId)
                        .executeUpdate() == 1);
    }

    private boolean existsMessage(Connection connection, String queueName, String messageId) {
        String EXISTS_MESSAGE = "SELECT EXISTS(SELECT 1 FROM queue_message WHERE queue_name = ? AND message_id = ?)";
        return query(connection, EXISTS_MESSAGE, q -> q.addParameter(queueName).addParameter(messageId).exists());
    }

    private void pushMessage(Connection connection, String queueName, String messageId, String payload, long offsetTimeInSecond) {
        String PUSH_MESSAGE = "INSERT INTO queue_message (created_on, deliver_on, queue_name, message_id, offset_time_seconds, payload) VALUES (?, ?, ?, ?, ?, ?)";
        String UPDATE_MESSAGE = "UPDATE queue_message SET payload = ? WHERE queue_name = ? AND message_id = ?";

        createQueueIfNotExists(connection, queueName);

        Date now = DateUtils.truncate(new Date(), Calendar.SECOND);
        Date deliverTime = new Date(now.getTime() + (offsetTimeInSecond * 1_000));
        boolean exists = existsMessage(connection, queueName, messageId);

        if (!exists) {
            execute(connection, PUSH_MESSAGE, q ->
                    q.addTimestampParameter(now)
                    .addTimestampParameter(deliverTime)
                    .addParameter(queueName)
                    .addParameter(messageId)
                    .addParameter(offsetTimeInSecond)
                    .addParameter(payload).executeUpdate());

        } else {
            execute(connection, UPDATE_MESSAGE, q ->
                    q.addParameter(payload)
                            .addParameter(queueName)
                            .addParameter(messageId)
                            .executeUpdate());
        }
    }

    private void removeMessage(Connection connection, String queueName, String messageId) {
        String REMOVE_MESSAGE = "DELETE FROM queue_message WHERE queue_name = ? AND message_id = ?";
        execute(connection, REMOVE_MESSAGE, q -> q.addParameter(queueName).addParameter(messageId).executeDelete());
    }

    private List<String> peekMessages(String queueName, int count) {
        return getWithTransaction(tx -> peekMessages(tx, queueName, count));
    }

    private List<String> peekMessages(Connection connection, String queueName, int count) {
        if (count < 1) return Collections.emptyList();

        final long peekTime = System.currentTimeMillis() + 1;

        String PEEK_MESSAGES = "SELECT message_id FROM queue_message WHERE queue_name = ? AND popped = false AND deliver_on <= TIMESTAMP(?) ORDER BY deliver_on, created_on LIMIT ?";
        return query(connection, PEEK_MESSAGES, q -> q.addParameter(queueName).addTimestampParameter(peekTime).addParameter(count).executeScalarList(String.class));
    }

    private List<String> popMessages(Connection connection, String queueName, List<String> messageIds) {
        if (messageIds.isEmpty()) {
            return messageIds;
        }

        final String POP_MESSAGES = "UPDATE queue_message SET popped = true WHERE queue_name = ? AND message_id IN (%s) AND popped = false";
        final String query = String.format(POP_MESSAGES, Query.generateInBindings(messageIds.size()));

        int result = query(connection, query, q -> q.addParameter(queueName).addParameters(messageIds).executeUpdate());

        if (result != messageIds.size()) {
            String message = String.format("could not pop all messages for given ids: %s (%d messages were popped)", messageIds, result);
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, message);
        }

        return messageIds;
    }

    private List<Message> readMessages(Connection connection, String queueName, List<String> messageIds) {
        if (messageIds.isEmpty()) { return Collections.emptyList(); }

        final String READ_MESSAGES = "SELECT message_id, payload FROM queue_message WHERE queue_name = ? AND message_id IN (%s) AND popped = false ORDER BY deliver_on, created_on";
        final String query = String.format(READ_MESSAGES, Query.generateInBindings(messageIds.size()));

        List<Message> messages = query(connection, query, p ->
                p.addParameter(queueName)
                .addParameters(messageIds).executeAndFetch(rs -> {
                    List<Message> results = new ArrayList<>();
                    while(rs.next()) {
                        Message m = new Message();
                        m.setId(rs.getString("message_id"));
                        m.setPayload(rs.getString("payload"));
                        results.add(m);
                    }
                    return results;
                }));


        if (messages.size() != messageIds.size()) {
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, "could not read all messages for given ids: " + messageIds);
        }

        return messages;
    }

    private void createQueueIfNotExists(Connection connection, String queueName) {
        String EXISTS_QUEUE = "SELECT EXISTS(SELECT 1 FROM queue WHERE queue_name = ?)";
        boolean queueExists = query(connection, EXISTS_QUEUE, q -> q.addParameter(queueName).exists());

        if (!queueExists) {
            logger.debug("creating queue {}", queueName);
            String CREATE_QUEUE = "INSERT INTO queue (queue_name) VALUES (?)";
            execute(connection, CREATE_QUEUE, q -> q.addParameter(queueName).executeUpdate());
        }
    }
}
