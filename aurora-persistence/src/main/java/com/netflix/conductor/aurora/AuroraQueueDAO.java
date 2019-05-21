package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.aurora.sql.Query;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.QueueDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AuroraQueueDAO extends AuroraBaseDAO implements QueueDAO {
	private static final Logger logger = LoggerFactory.getLogger(AuroraQueueDAO.class);
	private static final Long UNACK_SCHEDULE_MS = 60_000L;
	private static final Long UNACK_TIME_MS = 60_000L;
	private final int stalePeriod;

	public AuroraQueueDAO(DataSource dataSource, ObjectMapper mapper, Configuration config) {
		super(dataSource, mapper);
		stalePeriod = config.getIntProperty("workflow.aurora.stale.period.seconds", 60);

		Executors.newSingleThreadScheduledExecutor()
			.scheduleAtFixedRate(this::processAllUnacks,
				UNACK_SCHEDULE_MS, UNACK_SCHEDULE_MS, TimeUnit.MILLISECONDS);
	}

	@Override
	public void push(String queueName, String id, long offsetTimeInSecond) {
		withTransaction(tx -> pushMessage(tx, queueName, id, null, offsetTimeInSecond));
	}

	@Override
	public void push(String queueName, List<Message> messages) {
		withTransaction(tx -> messages
			.forEach(message -> pushMessage(tx, queueName, message.getId(), message.getPayload(), 0)));
	}

	@Override
	public boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond) {
		return getWithTransaction(tx -> {
			if (!existsMessage(tx, queueName, id)) {
				pushMessage(tx, queueName, id, null, offsetTimeInSecond);
				return true;
			}
			return false;
		});
	}

	@Override
	public List<String> pop(String queueName, int count, int timeout) {
		List<Message> messages = getWithTransactionWithOutErrorPropagation(tx -> popMessages(tx, queueName, count, timeout));
		if (messages == null) return new ArrayList<>();
		return messages.stream().map(Message::getId).collect(Collectors.toList());
	}

	@Override
	public List<Message> pollMessages(String queueName, int count, int timeout) {
		List<Message> messages = getWithTransactionWithOutErrorPropagation(tx -> popMessages(tx, queueName, count, timeout));
		if (messages == null) return new ArrayList<>();
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
	public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
		long offsetSeconds = unackTimeout / 1000;

		final String SQL = "UPDATE queue_message SET offset_time_seconds = ?, deliver_on = now() + interval '" + offsetSeconds + " second' " +
			" WHERE queue_name = ? AND message_id = ?";

		return queryWithTransaction(SQL,
			q -> q.addParameter(offsetSeconds)
				.addParameter(queueName)
				.addParameter(messageId).executeUpdate()) == 1;
	}

	@Override
	public void flush(String queueName) {
		final String SQL = "DELETE FROM queue_message WHERE queue_name = ?";
		executeWithTransaction(SQL, q -> q.addParameter(queueName).executeDelete());
	}

	@Override
	public Map<String, Long> queuesDetail() {
		final String SQL = "SELECT queue_name, (SELECT count(*) FROM queue_message WHERE popped = false AND queue_name = q.queue_name) AS size FROM queue q";
		return queryWithTransaction(SQL, q -> q.executeAndFetch(rs -> {
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
		final String SQL = "SELECT queue_name, \n"
			+ "       (SELECT count(*) FROM queue_message WHERE popped = false AND queue_name = q.queue_name) AS size,\n"
			+ "       (SELECT count(*) FROM queue_message WHERE popped = true AND queue_name = q.queue_name) AS uacked \n"
			+ "FROM queue q";

		return queryWithTransaction(SQL, q -> q.executeAndFetch(rs -> {
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
	public void processUnacks(String queueName) {
		final String PROCESS_UNACKS = "UPDATE queue_message SET popped = false WHERE queue_name = ? AND popped = true AND (now() + interval '60 second') > deliver_on";
		executeWithTransaction(PROCESS_UNACKS, q -> q.addParameter(queueName).executeUpdate());
	}

	@Override
	public boolean exists(String queueName, String id) {
		return false;
	}

	@Override
	public boolean wakeup(String queueName, String id) {
		return false;
	}

	private void processAllUnacks() {
		logger.trace("processAllUnacks started");

		final String PROCESS_ALL_UNACKS = "UPDATE queue_message SET popped = false WHERE popped = true AND (now() + interval '60 second') > deliver_on";
		executeWithTransaction(PROCESS_ALL_UNACKS, Query::executeUpdate);
	}

	private boolean existsMessage(Connection connection, String queueName, String messageId) {
		final String EXISTS_MESSAGE = "SELECT EXISTS(SELECT 1 FROM queue_message WHERE queue_name = ? AND message_id = ?)";
		return query(connection, EXISTS_MESSAGE, q -> q.addParameter(queueName).addParameter(messageId).exists());
	}

	private void pushMessage(Connection connection, String queueName, String messageId, String payload,
							 long offsetTimeInSecond) {

		String PUSH_MESSAGE = "INSERT INTO queue_message (deliver_on, queue_name, message_id, offset_time_seconds, payload) VALUES (TIMESTAMPADD(SECOND,?,CURRENT_TIMESTAMP), ?, ?,?,?) ON DUPLICATE KEY UPDATE payload=VALUES(payload), deliver_on=VALUES(deliver_on)";

		createQueueIfNotExists(connection, queueName);

		execute(connection, PUSH_MESSAGE, q -> q.addParameter(offsetTimeInSecond).addParameter(queueName)
			.addParameter(messageId).addParameter(offsetTimeInSecond).addParameter(payload).executeUpdate());

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

		final String SQL = "UPDATE queue_message SET popped = true WHERE queue_name = ? AND message_id IN (%s) AND popped = false";

		final List<String> Ids = messages.stream().map(Message::getId).collect(Collectors.toList());
		final String query = String.format(SQL, Query.generateInBindings(messages.size()));

		int result = query(connection, query, q -> q.addParameter(queueName).addParameters(Ids).executeUpdate());

		if (result != messages.size()) {
			String message = String.format("Could not pop all messages for given ids: %s (%d messages were popped)",
				Ids, result);
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, message);
		}
		return messages;
	}

	private List<Message> peekMessages(Connection connection, String queueName, int count) {
		if (count < 1)
			return Collections.emptyList();

		final String SQL = "SELECT message_id, payload FROM queue_message WHERE queue_name = ? AND popped = false AND deliver_on <= now() ORDER BY deliver_on, created_on LIMIT ?";

		return query(connection, SQL, p -> p.addParameter(queueName)
			.addParameter(count).executeAndFetch(rs -> {
				List<Message> results = new ArrayList<>();
				while (rs.next()) {
					Message m = new Message();
					m.setId(rs.getString("message_id"));
					m.setPayload(rs.getString("payload"));
					results.add(m);
				}
				return results;
			}));
	}

	private boolean removeMessage(Connection connection, String queueName, String messageId) {
		final String SQL = "DELETE FROM queue_message WHERE queue_name = ? AND message_id = ?";
		return query(connection, SQL,
			q -> q.addParameter(queueName).addParameter(messageId).executeDelete());
	}

	private void createQueueIfNotExists(Connection connection, String queueName) {
		final String SQL = "INSERT INTO queue (queue_name) VALUES (?) ON CONFLICT ON CONSTRAINT queue_name DO NOTHING";
		execute(connection, SQL, q -> q.addParameter(queueName).executeUpdate());
	}
}
