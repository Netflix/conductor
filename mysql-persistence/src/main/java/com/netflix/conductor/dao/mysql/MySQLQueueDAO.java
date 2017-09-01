package com.netflix.conductor.dao.mysql;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.join;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang.time.DateUtils;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.QueueDAO;

class MySQLQueueDAO extends MySQLBaseDAO implements QueueDAO {

	private static final String CREATE_QUEUE = "INSERT INTO queue (queue_name) VALUES (:queueName)";
	private static final String EXISTS_QUEUE = "SELECT EXISTS(SELECT 1 FROM queue WHERE queue_name = :queueName)";

	private static final String PUSH_MESSAGE = "INSERT INTO queue_message (created_on, deliver_on, queue_name, message_id, offset_time_seconds, payload) VALUES (:createdOn, :deliverOn, :queueName, :messageId, :offsetSeconds, :payload)";
	private static final String EXISTS_MESSAGE = "SELECT EXISTS(SELECT 1 FROM queue_message WHERE queue_name = :queueName AND message_id = :messageId)";
	private static final String REMOVE_MESSAGE = "DELETE FROM queue_message WHERE queue_name = :queueName AND message_id = :messageId";
	private static final String FLUSH_QUEUE = "DELETE FROM queue_message WHERE queue_name = :queueName";

	private static final String PEEK_MESSAGES = "SELECT message_id FROM queue_message WHERE queue_name = :queueName LIMIT :count";
	private static final String POP_MESSAGES = "UPDATE queue_message SET popped = true WHERE queue_name = :queueName AND message_id IN (%s)";
	private static final String READ_MESSAGES = "SELECT message_id, payload FROM queue_message WHERE queue_name = :queueName AND message_id IN (%s)";

	private static final String GET_QUEUE_SIZE = "SELECT COUNT(*) FROM queue_message WHERE queue_name = :queueName";
	private static final String GET_QUEUES_DETAIL =
			"SELECT queue_name, \n" +
			" (SELECT count(*) FROM queue_message WHERE popped = false AND queue_name = q.queue_name) AS size,\n" +
			" (SELECT count(*) FROM queue_message WHERE popped = true AND queue_name = q.queue_name) AS uacked \n" +
			"FROM queue q";

	@Inject
	MySQLQueueDAO(ObjectMapper om, Sql2o sql2o) {
		super(om, sql2o);
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
		List<String> poppedMessageIds = pop(queueName, count, timeout);
		return readMessages(queueName, poppedMessageIds);
	}

	@Override
	public void remove(String queueName, String messageId) {
		withTransaction(tx -> removeMessage(tx, queueName, messageId));
	}

	@Override
	public int getSize(String queueName) {
		return getWithTransaction(tx -> tx.createQuery(GET_QUEUE_SIZE).addParameter("queueName", queueName).executeScalar(Integer.class));
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
		return false;
	}

	@Override
	public void flush(String queueName) {
		withTransaction(tx -> tx.createQuery(FLUSH_QUEUE).addParameter("queueName", queueName).executeUpdate());
	}

	@Override
	public Map<String, Long> queuesDetail() {
		Map<String, Long> detail = Maps.newHashMap();
		withTransaction(tx -> tx.createQuery(GET_QUEUES_DETAIL).executeAndFetchTable().asList().forEach(row -> {
			String queueName = (String)row.get("queue_name");
			Number queueSize = (Number)row.get("size");
			detail.put(queueName, queueSize.longValue());
		}));
		return detail;
	}

	@Override
	public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
		Map<String, Map<String, Map<String, Long>>> result = Maps.newHashMap();

		withTransaction(tx -> tx.createQuery(GET_QUEUES_DETAIL).executeAndFetchTable().asList().forEach(row -> {
			String queueName = (String)row.get("queue_name");
			Number queueSize = (Number)row.get("size");
			Number queueUnacked = (Number)row.get("uacked");
			result.put(queueName, ImmutableMap.of(
					"a", ImmutableMap.of(   //sharding not implemented, returning only one shard with all the info
						"size", queueSize.longValue(),
						"uacked", queueUnacked.longValue()
					)
				)
			);
		}));

		return result;
	}

	@Override
	public void processUnacks(String queueName) {

	}

	private boolean existsMessage(Connection connection, String queueName, String id) {
		return connection.createQuery(EXISTS_MESSAGE)
				.addParameter("queueName", queueName)
				.addParameter("messageId", id)
				.executeScalar(Boolean.class);
	}

	private void pushMessage(Connection connection, String queueName, String messageId, String payload, long offsetTimeInSecond) {
		createQueueIfNotExists(connection, queueName);
		Date now = DateUtils.truncate(new Date(), Calendar.SECOND);
		Date deliverTime = new Date(now.getTime() + (offsetTimeInSecond*1000));
		connection.createQuery(PUSH_MESSAGE)
				.addParameter("createdOn", now)
				.addParameter("deliverOn", deliverTime)
				.addParameter("queueName", queueName)
				.addParameter("messageId", messageId)
				.addParameter("offsetSeconds", offsetTimeInSecond)
				.addParameter("payload", payload).executeUpdate();
	}

	private void removeMessage(Connection connection, String queueName, String messageId) {
		connection.createQuery(REMOVE_MESSAGE).addParameter("queueName", queueName).addParameter("messageId", messageId).executeUpdate();
	}

	private List<String> peekMessages(String queueName, int count) {
		if (count < 1) return Collections.emptyList();
		return getWithTransaction(tx -> tx.createQuery(PEEK_MESSAGES)
				.addParameter("queueName", queueName)
				.addParameter("count", count)
				.executeScalarList(String.class));
	}

	private List<String> popMessages(Connection connection, String queueName, List<String> messageIds) {
		if (messageIds.isEmpty()) return messageIds;
		List<String> stringIds = messageIds.stream().map(id -> format("'%s'", id)).collect(Collectors.toList());
		String query = format(POP_MESSAGES, join(stringIds, ","));
		int result = connection.createQuery(query).addParameter("queueName", queueName).executeUpdate().getResult();
		if (result != messageIds.size()) {
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, "could not pop all messages for given ids: " + messageIds);
		}
		return messageIds;
	}

	private List<Message> readMessages(String queueName, List<String> messageIds) {
		if (messageIds.isEmpty()) return Collections.emptyList();
		List<String> stringIds = messageIds.stream().map(id -> format("'%s'", id)).collect(Collectors.toList());
		String query = format(READ_MESSAGES, join(stringIds, ","));
		List<Message> messages = getWithTransaction(tx -> tx.createQuery(query)
				.addParameter("queueName", queueName)
				.addColumnMapping("message_id", "id")
				.executeAndFetch(Message.class));
		if (messages.size() != messageIds.size()) {
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, "could not read all messages for given ids: " + messageIds);
		}
		return messages;
	}

	private void createQueueIfNotExists(Connection connection, String queueName) {
		boolean queueExists = connection.createQuery(EXISTS_QUEUE).addParameter("queueName", queueName).executeScalar(Boolean.class);
		if (!queueExists) {
			logger.info("creating queue {}", queueName);
			connection.createQuery(CREATE_QUEUE).addParameter("queueName", queueName).executeUpdate();
		}
	}
}
