package com.netflix.conductor.dao.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.QueueDAO;

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
public abstract class SQLQueueDAO extends SQLBaseDAO implements QueueDAO {
    private static final Long UNACK_SCHEDULE_MS = 60_000L;

    @Inject
    public SQLQueueDAO(ObjectMapper om, DataSource ds) {
        super(om, ds);

        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(this::processAllUnacks,
                        UNACK_SCHEDULE_MS, UNACK_SCHEDULE_MS, TimeUnit.MILLISECONDS);
        logger.debug(SQLQueueDAO.class.getName() + " is ready to serve");
    }

    @Override
    public void push(String queueName, String messageId, long offsetTimeInSecond) {
        push(queueName, messageId, 0, offsetTimeInSecond);
    }

    @Override
    public void push(String queueName, String messageId, int priority, long offsetTimeInSecond) {
        withTransaction(tx -> pushMessage(tx, queueName, messageId, null, priority, offsetTimeInSecond));
    }

    @Override
    public void push(String queueName, List<Message> messages) {
        withTransaction(tx -> messages
          .forEach(message -> pushMessage(tx, queueName, message.getId(), message.getPayload(), message.getPriority(), 0)));
    }

    @Override
    public boolean pushIfNotExists(String queueName, String messageId, long offsetTimeInSecond) {
        return pushIfNotExists(queueName, messageId, 0, offsetTimeInSecond);
    }

    @Override
    public boolean pushIfNotExists(String queueName, String messageId, int priority, long offsetTimeInSecond) {
        return getWithTransaction(tx -> {
            if (!existsMessage(tx, queueName, messageId)) {
                pushMessage(tx, queueName, messageId, null, priority, offsetTimeInSecond);
                return true;
            }
            return false;
        });
    }

    @Override
    public List<String> pop(String queueName, int count, int timeout) {
        List<Message> messages = getWithTransactionWithOutErrorPropagation(tx -> popMessages(tx, queueName, count, timeout));
        if(messages == null) return new ArrayList<>();
        return messages.stream().map(Message::getId).collect(Collectors.toList());
    }

    @Override
    public List<Message> pollMessages(String queueName, int count, int timeout) {
        List<Message> messages = getWithTransactionWithOutErrorPropagation(tx -> popMessages(tx, queueName, count, timeout));
        if(messages == null) return new ArrayList<>();
        return messages;
    }

    @Override
    public void remove(String queueName, String messageId) {
        withTransaction(tx -> removeMessage(tx, queueName, messageId));
    }

    @Override
    public int getSize(String queueName) {
        final String GET_QUEUE_SIZE = "SELECT COUNT(*) FROM queue_message WHERE queue_name = ?";
        return queryWithTransaction(GET_QUEUE_SIZE, q -> ((Long) q.addParameter(queueName).executeCount()).intValue());
    }

    @Override
    public boolean ack(String queueName, String messageId) {
        return getWithTransaction(tx -> removeMessage(tx, queueName, messageId));
    }

    @Override
    public abstract boolean setUnackTimeout(String queueName, String messageId, long unackTimeout);

    @Override
    public void flush(String queueName) {
        final String FLUSH_QUEUE = "DELETE FROM queue_message WHERE queue_name = ?";
        executeWithTransaction(FLUSH_QUEUE, q -> q.addParameter(queueName).executeDelete());
    }

    @Override
    public Map<String, Long> queuesDetail() {
        final String GET_QUEUES_DETAIL = "SELECT queue_name, (SELECT count(*) FROM queue_message WHERE popped = false AND queue_name = q.queue_name) AS size FROM queue q";
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
        final String GET_QUEUES_DETAIL_VERBOSE = "SELECT queue_name, \n"
                + "       (SELECT count(*) FROM queue_message WHERE popped = false AND queue_name = q.queue_name) AS size,\n"
                + "       (SELECT count(*) FROM queue_message WHERE popped = true AND queue_name = q.queue_name) AS uacked \n"
                + "FROM queue q";
        // @formatter:on

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
    public abstract void processAllUnacks();

    @Override
    public abstract void processUnacks(String queueName);

    @Override
    public abstract boolean setOffsetTime(String queueName, String messageId, long offsetTimeInSecond);

    @Override
    public boolean exists(String queueName, String messageId) {
        return getWithTransaction(tx -> existsMessage(tx, queueName, messageId));
    }

    private boolean existsMessage(Connection connection, String queueName, String messageId) {
        final String EXISTS_MESSAGE = "SELECT EXISTS(SELECT 1 FROM queue_message WHERE queue_name = ? AND message_id = ?)";
        return query(connection, EXISTS_MESSAGE, q -> q.addParameter(queueName).addParameter(messageId).exists());
    }

    protected abstract void pushMessage(Connection connection, String queueName, String messageId, String payload,  Integer priority,
                             long offsetTimeInSecond);

    private boolean removeMessage(Connection connection, String queueName, String messageId) {
        final String REMOVE_MESSAGE = "DELETE FROM queue_message WHERE queue_name = ? AND message_id = ?";
        return query(connection, REMOVE_MESSAGE,
                q -> q.addParameter(queueName).addParameter(messageId).executeDelete());
    }

    protected abstract List<Message> peekMessages(Connection connection, String queueName, int count);

    private List<Message> popMessages(Connection connection, String queueName, int count, int timeout) {
        long start = System.currentTimeMillis();
        List<Message> messages = peekMessages(connection, queueName, count);

        while (messages.size() < count && ((System.currentTimeMillis() - start) < timeout)) {
            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
            messages = peekMessages(connection, queueName, count);
        }

        if (messages.isEmpty()) {
            return messages;
        }

        final String POP_MESSAGES = "UPDATE queue_message SET popped = true WHERE queue_name = ? AND message_id IN (%s) AND popped = false";

        final List<String> Ids = messages.stream().map(Message::getId).collect(Collectors.toList());
        final String query = String.format(POP_MESSAGES, Query.generateInBindings(messages.size()));

        int result = query(connection, query, q -> q.addParameter(queueName).addParameters(Ids).executeUpdate());

        if (result != messages.size()) {
            String message = String.format("Could not pop all messages for given ids: %s (%d messages were popped)",
                    Ids, result);
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, message);
        }
        return messages;
    }

    protected abstract void createQueueIfNotExists(Connection connection, String queueName);
}
