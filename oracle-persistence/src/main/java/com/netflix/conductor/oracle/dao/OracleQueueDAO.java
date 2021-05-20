/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.oracle.dao;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.oracle.util.Query;

import oracle.jdbc.OracleResultSet;

public class OracleQueueDAO extends OracleBaseDAO implements QueueDAO {

    private static final Long UNACK_SCHEDULE_MS = 60_000L;

    public OracleQueueDAO(ObjectMapper objectMapper, DataSource dataSource) {
        super(objectMapper, dataSource);

        Executors.newSingleThreadScheduledExecutor()
            .scheduleAtFixedRate(this::processAllUnacks,
                UNACK_SCHEDULE_MS, UNACK_SCHEDULE_MS, TimeUnit.MILLISECONDS);
        logger.debug(OracleQueueDAO.class.getName() + " is ready to serve");
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
            .forEach(message -> pushMessage(tx, queueName, message.getId(), message.getPayload(), message.getPriority(),
                0)));
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
    	List<Message> messages = getWithTransactionWithOutErrorPropagation(
            tx -> popMessages(tx, queueName, count, timeout));
        if (messages == null) {
            return new ArrayList<>();
        }
        return messages.stream().map(Message::getId).collect(Collectors.toList());
    }

    @Override
    public List<Message> pollMessages(String queueName, int count, int timeout) {
    	List<Message> messages = getWithTransactionWithOutErrorPropagation(
            tx -> popMessages(tx, queueName, count, timeout));
        if (messages == null) {
            return new ArrayList<>();
        }
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

        final String UPDATE_UNACK_TIMEOUT = "UPDATE queue_message SET offset_time_seconds = ?, deliver_on = (CURRENT_TIMESTAMP + NUMTODSINTERVAL(?,'SECOND')) WHERE queue_name = ? AND message_id = ?";

        return queryWithTransaction(UPDATE_UNACK_TIMEOUT,
            q -> q.addParameter(updatedOffsetTimeInSecond).addParameter(updatedOffsetTimeInSecond)
                .addParameter(queueName).addParameter(messageId).executeUpdate()) == 1;
    }

    @Override
    public void flush(String queueName) {
        final String FLUSH_QUEUE = "DELETE FROM queue_message WHERE queue_name = ?";
        executeWithTransaction(FLUSH_QUEUE, q -> q.addParameter(queueName).executeDelete());
    }

    @Override
    public Map<String, Long> queuesDetail() {
        final String GET_QUEUES_DETAIL = "SELECT queue_name, (SELECT count(*) FROM queue_message WHERE popped = 'N' AND queue_name = q.queue_name) AS size FROM queue q";
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
            + "       (SELECT count(*) FROM queue_message WHERE popped = 'N' AND queue_name = q.queue_name) AS size,\n"
            + "       (SELECT count(*) FROM queue_message WHERE popped = 'Y' AND queue_name = q.queue_name) AS uacked \n"
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
     *
     * @since 1.11.6
     */
    public void processAllUnacks() {

        logger.trace("processAllUnacks started");

        final String PROCESS_ALL_UNACKS = "UPDATE queue_message SET popped = 'N' WHERE popped = 'Y' AND (CURRENT_TIMESTAMP - NUMTODSINTERVAL(60,'SECOND')) > deliver_on";
        executeWithTransaction(PROCESS_ALL_UNACKS, Query::executeUpdate);
    }

    @Override
    public void processUnacks(String queueName) {
        final String PROCESS_UNACKS = "UPDATE queue_message SET popped = 'N' WHERE queue_name = ? AND popped = 'Y' AND (CURRENT_TIMESTAMP - NUMTODSINTERVAL(60,'SECOND'))  > deliver_on";
        executeWithTransaction(PROCESS_UNACKS, q -> q.addParameter(queueName).executeUpdate());
    }

    @Override
    public boolean resetOffsetTime(String queueName, String messageId) {
        long offsetTimeInSecond = 0;    // Reset to 0
        final String SET_OFFSET_TIME =
            "UPDATE queue_message SET offset_time_seconds = ?, deliver_on = (CURRENT_TIMESTAMP + NUMTODSINTERVAL(?,'SECOND')) \n"
                + "WHERE queue_name = ? AND message_id = ?";

        return queryWithTransaction(SET_OFFSET_TIME, q -> q.addParameter(offsetTimeInSecond)
            .addParameter(offsetTimeInSecond).addParameter(queueName).addParameter(messageId).executeUpdate() == 1);
    }

    private boolean existsMessage(Connection connection, String queueName, String messageId) {
        final String EXISTS_MESSAGE = "SELECT 1 FROM DUAL WHERE EXISTS(SELECT 1 FROM queue_message WHERE queue_name = ? AND message_id = ?)";
        return query(connection, EXISTS_MESSAGE, q -> q.addParameter(queueName).addParameter(messageId).exists());
    }

    private void pushMessage(Connection connection, String queueName, String messageId, String payload,
        Integer priority,
        long offsetTimeInSecond) {

        createQueueIfNotExists(connection, queueName);

        String UPDATE_MESSAGE = "UPDATE queue_message SET payload=utl_raw.cast_to_raw(?), deliver_on=(CURRENT_TIMESTAMP + NUMTODSINTERVAL(?,'SECOND')) WHERE queue_name = ? AND message_id = ?";
        int rowsUpdated = query(connection, UPDATE_MESSAGE,
            q -> q.addParameter(payload).addParameter(offsetTimeInSecond)
                .addParameter(queueName).addParameter(messageId).executeUpdate());

        if (rowsUpdated == 0) {
            String PUSH_MESSAGE = "INSERT INTO queue_message(deliver_on, offset_time_seconds, queue_name, message_id, priority, payload) VALUES ((CURRENT_TIMESTAMP + NUMTODSINTERVAL(?,'SECOND')), ?, ?, ?, ?, utl_raw.cast_to_raw(?))";
            execute(connection, PUSH_MESSAGE, q -> 
            q.addParameter(offsetTimeInSecond)
            .addParameter(offsetTimeInSecond)
            .addParameter(queueName)
            .addParameter(messageId)
            .addParameter(priority)
            .addParameter(payload).executeUpdate());
        }
    }

    private boolean removeMessage(Connection connection, String queueName, String messageId) {
        final String REMOVE_MESSAGE = "DELETE FROM queue_message WHERE queue_name = ? AND message_id = ?";
        return query(connection, REMOVE_MESSAGE,
            q -> q.addParameter(queueName).addParameter(messageId).executeDelete());
    }

    private List<Message> peekMessages(Connection connection, String queueName, int count) {
    	if (count < 1) {
            return Collections.emptyList();
        }

        final String PEEK_MESSAGES = "SELECT /*+ INDEX(queue_message combo_queue_message) */ message_id, priority, payload FROM queue_message WHERE queue_name = ? AND popped = 'N' AND deliver_on <= (CURRENT_TIMESTAMP + NUMTODSINTERVAL(1/1000000,'SECOND')) ORDER BY priority DESC, deliver_on, created_on FETCH FIRST ? ROWS ONLY";

        return query(connection, PEEK_MESSAGES, p -> p.addParameter(queueName)
            .addParameter(count).executeAndFetch(rs -> {
                List<Message> results = new ArrayList<>();
                while (rs.next()) {
                    Message m = new Message();
                    m.setId(rs.getString("message_id"));
                    m.setPriority(rs.getInt("priority"));
                    try {
                    	OracleResultSet oracleResultSet = rs.unwrap(OracleResultSet.class);
                    	Blob blob = oracleResultSet.getBlob("payload");
                        if(null!=blob) {
                        	StringBuilder textBuilder = new StringBuilder();
                            try (Reader reader = new BufferedReader(new InputStreamReader
                              (blob.getBinaryStream(), Charset.forName(StandardCharsets.UTF_8.name())))) {
                                int c = 0;
                                while ((c = reader.read()) != -1) {
                                    textBuilder.append((char) c);
                                }
                            }
                           m.setPayload(textBuilder.toString());
                        }
                    }
                    catch(Exception e) {
                    	e.printStackTrace();
                    	m.setPayload(rs.getObject("payload").toString());
                    }
                    results.add(m);
                }
                return results;
            }));
    }

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

        List<Message> poppedMessages = new ArrayList<>();
        for (Message message : messages) {
            final String POP_MESSAGE = "UPDATE queue_message SET popped = 'Y' WHERE queue_name = ? AND message_id = ? AND popped = 'N'";
            int result = query(connection, POP_MESSAGE,
                q -> q.addParameter(queueName).addParameter(message.getId()).executeUpdate());

            if (result == 1) {
                poppedMessages.add(message);
            }
        }
        return poppedMessages;
    }

    private void createQueueIfNotExists(Connection connection, String queueName) {
        logger.trace("Creating new queue '{}'", queueName);
        final String EXISTS_QUEUE = "SELECT 1 FROM DUAL WHERE EXISTS(SELECT 1 FROM queue WHERE queue_name = ?)";
        boolean exists = query(connection, EXISTS_QUEUE, q -> q.addParameter(queueName).exists());
        if (!exists) {
            final String CREATE_QUEUE = "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX (queue (queue_name)) */ INTO queue (queue_name) VALUES (?)";
            execute(connection, CREATE_QUEUE, q -> q.addParameter(queueName).executeUpdate());
        }
    }

    @Override
    public boolean containsMessage(String queueName, String messageId) {
        final String EXISTS_QUEUE = "SELECT 1 FROM DUAL WHERE EXISTS(SELECT 1 FROM queue_message WHERE queue_name = ? AND message_id = ? )";
        return queryWithTransaction(EXISTS_QUEUE,
            q -> q.addParameter(queueName).addParameter(messageId).exists());
    }
}
