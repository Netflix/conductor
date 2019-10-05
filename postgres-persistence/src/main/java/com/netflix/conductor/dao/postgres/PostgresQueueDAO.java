package com.netflix.conductor.dao.postgres;

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
public class PostgresQueueDAO extends SQLQueueDAO {

    @Inject
    public PostgresQueueDAO(ObjectMapper om, DataSource ds) {
        super(om, ds);
    }

    @Override
    public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
        long updatedOffsetTimeInSecond = unackTimeout / 1000;

        final String UPDATE_UNACK_TIMEOUT = "UPDATE queue_message SET offset_time_seconds = ?, deliver_on = (current_timestamp + (? ||' seconds')::interval) WHERE queue_name = ? AND message_id = ?";

        return queryWithTransaction(UPDATE_UNACK_TIMEOUT,
                q -> q.addParameter(updatedOffsetTimeInSecond).addParameter(updatedOffsetTimeInSecond)
                        .addParameter(queueName).addParameter(messageId).executeUpdate()) == 1;
    }

    /**
     * Un-pop all un-acknowledged messages for all queues.
     */
    public void processAllUnacks() {

        logger.trace("processAllUnacks started");


        final String PROCESS_ALL_UNACKS = "UPDATE queue_message SET popped = false WHERE popped = true AND (current_timestamp + (60 ||' seconds')::interval) > deliver_on";
        executeWithTransaction(PROCESS_ALL_UNACKS, Query::executeUpdate);
    }

    @Override
    public void processUnacks(String queueName) {
        final String PROCESS_UNACKS = "UPDATE queue_message SET popped = false WHERE queue_name = ? AND popped = true AND (current_timestamp + (60 ||' seconds')::interval)  > deliver_on";
        executeWithTransaction(PROCESS_UNACKS, q -> q.addParameter(queueName).executeUpdate());
    }

    @Override
    public boolean setOffsetTime(String queueName, String messageId, long offsetTimeInSecond) {
        final String SET_OFFSET_TIME = "UPDATE queue_message SET offset_time_seconds = ?, deliver_on = (current_timestamp + (? ||' seconds')::interval) \n"
                + "WHERE queue_name = ? AND message_id = ?";

        return queryWithTransaction(SET_OFFSET_TIME, q -> q.addParameter(offsetTimeInSecond)
                .addParameter(offsetTimeInSecond).addParameter(queueName).addParameter(messageId).executeUpdate() == 1);
    }

    protected void pushMessage(Connection connection, String queueName, String messageId, String payload, Integer priority,
                             long offsetTimeInSecond) {

        String PUSH_MESSAGE = "INSERT INTO queue_message (deliver_on, queue_name, message_id, priority, offset_time_seconds, payload) VALUES ((current_timestamp + (? ||' seconds')::interval), ?,?,?,?,?) ON CONFLICT (queue_name,message_id) DO UPDATE SET payload=excluded.payload, deliver_on=excluded.deliver_on";

        createQueueIfNotExists(connection, queueName);

        execute(connection, PUSH_MESSAGE, q -> q.addParameter(offsetTimeInSecond).addParameter(queueName)
                .addParameter(messageId).addParameter(priority).addParameter(offsetTimeInSecond)
                .addParameter(payload).executeUpdate());

    }

    protected List<Message> peekMessages(Connection connection, String queueName, int count) {
        if (count < 1)
            return Collections.emptyList();

        final String PEEK_MESSAGES = "SELECT message_id, priority, payload FROM queue_message WHERE queue_name = ? AND popped = false AND deliver_on <= (current_timestamp + (1000 ||' microseconds')::interval) ORDER BY priority DESC, deliver_on, created_on LIMIT ?";

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
        final String CREATE_QUEUE = "INSERT INTO queue (queue_name) VALUES (?) ON CONFLICT (queue_name) DO NOTHING";
        execute(connection, CREATE_QUEUE, q -> q.addParameter(queueName).executeUpdate());
    }
}
