package com.netflix.conductor.dao.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.sql.Query;
import com.netflix.conductor.dao.sql.SQLQueueDAO;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Singleton
public class MySQLQueueDAO extends SQLQueueDAO {

    @Inject
    public MySQLQueueDAO(ObjectMapper om, DataSource ds) {
        super(om, ds);

        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(this::processAllUnacks,
                        UNACK_SCHEDULE_MS, UNACK_SCHEDULE_MS, TimeUnit.MILLISECONDS);
        logger.debug(MySQLQueueDAO.class.getName() + " is ready to serve");
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
        return getWithRetriedTransactions(tx -> {
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
        return getWithRetriedTransactions(tx -> removeMessage(tx, queueName, messageId));
    }

    @Override
    public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
        long updatedOffsetTimeInSecond = unackTimeout / 1000;

        final String UPDATE_UNACK_TIMEOUT = "UPDATE queue_message SET offset_time_seconds = ?, deliver_on = TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP) WHERE queue_name = ? AND message_id = ?";

        return queryWithTransaction(UPDATE_UNACK_TIMEOUT,
                q -> q.addParameter(updatedOffsetTimeInSecond).addParameter(updatedOffsetTimeInSecond)
                        .addParameter(queueName).addParameter(messageId).executeUpdate()) == 1;
    }

    /**
     * Un-pop all un-acknowledged messages for all queues.

     * @since 1.11.6
     */
    public void processAllUnacks() {

        logger.trace("processAllUnacks started");

        final String PROCESS_ALL_UNACKS = "UPDATE queue_message SET popped = false WHERE popped = true AND TIMESTAMPADD(SECOND,60,CURRENT_TIMESTAMP) > deliver_on";
        executeWithTransaction(PROCESS_ALL_UNACKS, Query::executeUpdate);
    }

    @Override
    public void processUnacks(String queueName) {
        final String PROCESS_UNACKS = "UPDATE queue_message SET popped = false WHERE queue_name = ? AND popped = true AND TIMESTAMPADD(SECOND,60,CURRENT_TIMESTAMP)  > deliver_on";
        executeWithTransaction(PROCESS_UNACKS, q -> q.addParameter(queueName).executeUpdate());
    }

    @Override
    public boolean setOffsetTime(String queueName, String messageId, long offsetTimeInSecond) {
        final String SET_OFFSET_TIME = "UPDATE queue_message SET offset_time_seconds = ?, deliver_on = TIMESTAMPADD(SECOND,?,CURRENT_TIMESTAMP) \n"
                + "WHERE queue_name = ? AND message_id = ?";

        return queryWithTransaction(SET_OFFSET_TIME, q -> q.addParameter(offsetTimeInSecond)
                .addParameter(offsetTimeInSecond).addParameter(queueName).addParameter(messageId).executeUpdate() == 1);
    }


    protected void pushMessage(Connection connection, String queueName, String messageId, String payload, Integer priority,
                             long offsetTimeInSecond) {

        String PUSH_MESSAGE = "INSERT INTO queue_message (deliver_on, queue_name, message_id, priority, offset_time_seconds, payload) VALUES (TIMESTAMPADD(SECOND,?,CURRENT_TIMESTAMP), ?, ?,?,?,?) ON DUPLICATE KEY UPDATE payload=VALUES(payload), deliver_on=VALUES(deliver_on)";

        createQueueIfNotExists(connection, queueName);

        execute(connection, PUSH_MESSAGE, q -> q.addParameter(offsetTimeInSecond).addParameter(queueName)
          .addParameter(messageId).addParameter(priority).addParameter(offsetTimeInSecond)
          .addParameter(payload).executeUpdate());
    }

    protected List<Message> peekMessages(Connection connection, String queueName, int count) {
        if (count < 1)
            return Collections.emptyList();

        final String PEEK_MESSAGES = "SELECT message_id, priority, payload FROM queue_message use index(combo_queue_message) WHERE queue_name = ? AND popped = false AND deliver_on <= TIMESTAMPADD(MICROSECOND, 1000, CURRENT_TIMESTAMP) ORDER BY priority DESC, deliver_on, created_on LIMIT ?";

        List<Message> messages = query(connection, PEEK_MESSAGES, p -> p.addParameter(queueName)
          .addParameter(count).executeAndFetch(rs -> {
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

    protected void createQueueIfNotExists(Connection connection, String queueName) {
        logger.trace("Creating new queue '{}'", queueName);
        final String CREATE_QUEUE = "INSERT IGNORE INTO queue (queue_name) VALUES (?)";
        execute(connection, CREATE_QUEUE, q -> q.addParameter(queueName).executeUpdate());
    }
}
