package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.QueueDAO;
import org.apache.commons.collections.CollectionUtils;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AuroraQueueDAO extends AuroraBaseDAO implements QueueDAO {
	private static final Set<String> queues = ConcurrentHashMap.newKeySet();
	private static final Long UNACK_SCHEDULE_MS = 5_000L;
	private static final Long UNACK_TIME_MS = 60_000L;
	private final int cluster_size;

	@Inject
	public AuroraQueueDAO(DataSource dataSource, ObjectMapper mapper, Configuration config) {
		super(dataSource, mapper);
		cluster_size = config.getIntProperty("conductor_cluster_size", 5);

		Executors.newSingleThreadScheduledExecutor()
			.scheduleWithFixedDelay(this::processAllUnacks, UNACK_SCHEDULE_MS, UNACK_SCHEDULE_MS, TimeUnit.MILLISECONDS);
	}

	@Override
	public void push(String queueName, String id, long offsetSeconds) {
		withTransaction(tx -> pushMessage(tx, queueName, id, null, offsetSeconds));
	}

	@Override
	public void push(String queueName, List<Message> messages) {
		withTransaction(tx -> messages
			.forEach(message -> pushMessage(tx, queueName, message.getId(), message.getPayload(), 0)));
	}

	@Override
	public boolean pushIfNotExists(String queueName, String id, long offsetSeconds) {
		return getWithTransaction(tx -> pushMessage(tx, queueName, id, null, offsetSeconds));
	}

	/**
	 * The plan is
	 * 1) Query eligible records including record id/version
	 * 2) Update popped,poppedOn,unackOn,version++ where id/version = id/version from first step
	 * 3) If update is success - then this session got the record, add it to the `foundIds`
	 * 4) Otherwise some other node took it as we are dealing in the multi-threaded world
	 * <p>
	 * Steps 2+3 must be in separate session
	 *
	 * @param queueName Name of the queue
	 * @param count     number of messages to be read from the queue
	 * @param timeout   timeout in milliseconds
	 * @return List of the message ids
	 */
	@Override
	public List<String> pop(String queueName, int count, int timeout) {
		createQueueIfNotExists(queueName);
		final String QUERY = "SELECT id, version, message_id FROM queue_message " +
			"WHERE queue_name = ? AND popped = false AND deliver_on < now() " +
			"ORDER BY deliver_on, version, id LIMIT ?";

		final String UPDATE = "UPDATE queue_message " +
			"SET popped = true, unack_on = ?, version = version + 1 " +
			"WHERE id = ? AND version = ?";

		// Pick up X times more ids than requested as we have X servers in the cluster
		// So each of them might get as much as possible
		// But result will be not greater than requested count
		final int limit = count * cluster_size;

		try (Connection tx = dataSource.getConnection()) {
			tx.setAutoCommit(false);

			long start = System.currentTimeMillis();
			Set<String> foundIds = new HashSet<>();

			// Returns true until foundIds = count or time spent = timeout
			Supplier<Boolean> keepPooling = () -> foundIds.size() < count
				&& ((System.currentTimeMillis() - start) < timeout);

			// Repeat until foundIds = count or time spent = timeout
			while (keepPooling.get()) {

				// Get the list of popped message ids
				List<QueueMessage> messages = query(tx, QUERY, q -> q
					.addParameter(queueName.toLowerCase())
					.addParameter(limit)
					.executeAndFetch(rs -> {
						List<QueueMessage> popped = new ArrayList<>(limit);
						while (rs.next()) {
							QueueMessage m = new QueueMessage();
							m.id = rs.getLong("id");
							m.version = rs.getLong("version");
							m.message_id = rs.getString("message_id");

							popped.add(m);
						}
						return popped;
					}));

				// Mark them as popped issuing commit after each of them
				for (QueueMessage m : messages) {

					// Shall we stop pooling?
					if (!keepPooling.get()) {
						return Lists.newArrayList(foundIds);
					}

					// Continue to lock an record
					long unack_on = System.currentTimeMillis() + UNACK_TIME_MS;
					try {
						int updated = query(tx, UPDATE, u -> u
							.addTimestampParameter(unack_on)
							.addParameter(m.id)
							.addParameter(m.version)
							.executeUpdate());

						// Means record being updated - we got it
						if (updated > 0) {
							foundIds.add(m.message_id);
						}

						tx.commit();
					} catch (Throwable th) {
						logger.error("pop: lock failed for {} with {}", queueName, th.getMessage(), th);
						try {
							tx.rollback();
						} catch (SQLException ex) {
							logger.error("pop: rollback failed for {} with {}", queueName, ex.getMessage(), ex);
						}
					}
				}

				Thread.sleep(10);
			}

			tx.commit();
			return Lists.newArrayList(foundIds);
		} catch (Exception ex) {
			logger.error("pop: failed for {} with {}", queueName, ex.getMessage(), ex);
		}

		return Collections.emptyList();
	}

	@Override
	public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
		long unack_on = System.currentTimeMillis() + unackTimeout; // now + timeout

		final String UPDATE = "UPDATE queue_message " +
			"SET popped = true, unack_on = ?, unacked = true, version = version + 1 " +
			"WHERE queue_name = ? AND message_id = ?";

		return queryWithTransaction(UPDATE,
			q -> q.addTimestampParameter(unack_on)
				.addParameter(queueName.toLowerCase())
				.addParameter(messageId)
				.executeUpdate()) == 1;
	}

	@Override
	public void processUnacks(String queueName) {
		long unack_on = System.currentTimeMillis();

		final String SQL = "UPDATE queue_message " +
			"SET popped = false, deliver_on = now(), unack_on = null, unacked = false, version = version + 1 " +
			"WHERE queue_name = ? AND popped = true AND unack_on < ?";

		executeWithTransaction(SQL, q -> q.addParameter(queueName.toLowerCase()).addTimestampParameter(unack_on).executeUpdate());
	}

	@Override
	@Deprecated
	public List<Message> pollMessages(String queueName, int count, int timeout) {
		List<String> ids = pop(queueName, count, timeout);
		if (CollectionUtils.isEmpty(ids))
			return new ArrayList<>();

		return getWithTransaction(tx -> ids.stream().map(id -> peekMessage(tx, queueName, id)).collect(Collectors.toList()));
	}

	@Override
	public void remove(String queueName, String messageId) {
		withTransaction(tx -> removeMessage(tx, queueName, messageId));
	}

	@Override
	public int getSize(String queueName) {
		final String SQL = "SELECT COUNT(*) FROM queue_message WHERE queue_name = ?";
		return queryWithTransaction(SQL, q -> ((Long) q.addParameter(queueName.toLowerCase()).executeCount()).intValue());
	}

	@Override
	public boolean ack(String queueName, String messageId) {
		return getWithTransaction(tx -> removeMessage(tx, queueName, messageId));
	}

	@Override
	public void flush(String queueName) {
		final String SQL = "DELETE FROM queue_message WHERE queue_name = ?";
		executeWithTransaction(SQL, q -> q.addParameter(queueName.toLowerCase()).executeDelete());
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
	public boolean exists(String queueName, String id) {
		return getWithTransaction(tx -> existsMessage(tx, queueName, id));
	}

	@Override
	public boolean wakeup(String queueName, String id) {
		createQueueIfNotExists(queueName);
		final String SQL = "SELECT * FROM queue_message WHERE queue_name = ? AND message_id = ?";

		QueueMessage record = queryWithTransaction(SQL, p -> p.addParameter(queueName.toLowerCase())
			.addParameter(id).executeAndFetch(rs -> {
				if (rs.next()) {
					QueueMessage m = new QueueMessage();
					m.id = rs.getLong("id");
					m.version = rs.getLong("version");
					m.queue_name = rs.getString("queue_name");
					m.message_id = rs.getString("message_id");
					m.payload = rs.getString("payload");
					m.popped = rs.getBoolean("popped");
					m.deliver_on = rs.getTimestamp("deliver_on");
					m.unack_on = rs.getTimestamp("unack_on");
					m.unacked = rs.getBoolean("unacked");

					return m;
				}
				return null;
			}));

		if (record == null) {
			logger.debug("wakeup no record exists for " + queueName + "/" + id);
			return pushIfNotExists(queueName, id, 0);
		}

		// pop happened but setUnackTimeout dit not yet - mostly means the record in the decider at this moment
		if (record.popped && !record.unacked) {
			logger.debug("wakeup record popped for " + queueName + "/" + id);
			return false;
		}

		// Otherwise make it visible right away
		final String UPDATE = "UPDATE queue_message " +
			"SET popped = false, deliver_on = ?, unack_on = null, unacked = false, version = version + 1 " +
			"WHERE id = ? AND version = ?";

		return queryWithTransaction(UPDATE, q -> q.addTimestampParameter(1L)
			.addParameter(record.id)
			.addParameter(record.version)
			.executeUpdate()) > 0;
	}

	private boolean existsMessage(Connection connection, String queueName, String messageId) {
		final String SQL = "SELECT true FROM queue_message WHERE queue_name = ? AND message_id = ?";
		return query(connection, SQL, q -> q.addParameter(queueName.toLowerCase()).addParameter(messageId).exists());
	}

	private boolean pushMessage(Connection connection, String queueName, String messageId, String payload, long offsetSeconds) {
		createQueueIfNotExists(queueName);

		String SQL = "INSERT INTO queue_message (queue_name, message_id, popped, deliver_on, payload) " +
			"VALUES (?, ?, ?, ?, ?) ON CONFLICT ON CONSTRAINT queue_name_msg DO NOTHING";

		long deliverOn = System.currentTimeMillis() + (offsetSeconds * 1000);

		return query(connection, SQL, q -> q.addParameter(queueName.toLowerCase())
			.addParameter(messageId)
			.addParameter(false)
			.addTimestampParameter(deliverOn)
			.addParameter(payload)
			.executeUpdate() > 0);
	}

	private Message peekMessage(Connection connection, String queueName, String messageId) {
		final String SQL = "SELECT message_id, payload FROM queue_message WHERE queue_name = ? AND message_id = ?";

		return query(connection, SQL, p -> p.addParameter(queueName.toLowerCase())
			.addParameter(messageId).executeAndFetch(rs -> {
				if (rs.next()) {
					Message m = new Message();
					m.setId(rs.getString("message_id"));
					m.setPayload(rs.getString("payload"));
					return m;
				}
				return null;
			}));
	}

	private boolean removeMessage(Connection connection, String queueName, String messageId) {
		final String SQL = "DELETE FROM queue_message WHERE queue_name = ? AND message_id = ?";
		return query(connection, SQL,
			q -> q.addParameter(queueName.toLowerCase()).addParameter(messageId).executeDelete());
	}

	private void createQueueIfNotExists(String queueName) {
		if (queues.contains(queueName)) {
			return;
		}
		final String SQL = "INSERT INTO queue (queue_name) VALUES (?) ON CONFLICT ON CONSTRAINT queue_name DO NOTHING";
		executeWithTransaction(SQL, q -> q.addParameter(queueName.toLowerCase()).executeUpdate());
		queues.add(queueName);
	}

	private void processAllUnacks() {
		long unack_on = System.currentTimeMillis();

		final String SQL = "UPDATE queue_message " +
			"SET popped = false, deliver_on = now(), unack_on = null, unacked = false, version = version + 1 " +
			"WHERE popped = true AND unack_on < ?";

		executeWithTransaction(SQL, q -> q.addTimestampParameter(unack_on).executeUpdate());
	}

	private static class QueueMessage {
		long id;
		long version;
		String queue_name;
		String message_id;
		String payload;
		boolean popped;
		boolean unacked;
		Timestamp deliver_on;
		Timestamp unack_on;
	}
}
