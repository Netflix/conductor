package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.QueueDAO;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

public class AuroraQueueDAO extends AuroraBaseDAO implements QueueDAO {
	private static final Logger logger = LoggerFactory.getLogger(AuroraQueueDAO.class);
	private static final Set<String> queues = ConcurrentHashMap.newKeySet();
	private static final Long UNACK_SCHEDULE_MS = 60_000L;
	private static final Long UNACK_TIME_MS = 60_000L;
	private static final long POPPED_THRESHOLD = 500; // TODO What is the best value ?
	private final int stalePeriod;

	@Inject
	public AuroraQueueDAO(DataSource dataSource, ObjectMapper mapper, Configuration config) {
		super(dataSource, mapper);
		stalePeriod = config.getIntProperty("workflow.aurora.stale.period.seconds", 60);

		Executors.newSingleThreadScheduledExecutor()
			.scheduleAtFixedRate(this::processAllUnacks, UNACK_SCHEDULE_MS, UNACK_SCHEDULE_MS, TimeUnit.MILLISECONDS);
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
		return getWithTransaction(tx -> {
			if (!existsMessage(tx, queueName, id)) {
				pushMessage(tx, queueName, id, null, offsetSeconds);
				return true;
			}
			return false;
		});
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

		try {
			long start = System.currentTimeMillis();
			Set<String> foundIds = new HashSet<>();

			final String QUERY = "SELECT id, message_id, version FROM queue_message " +
				"WHERE queue_name = ? AND popped = false AND deliver_on < now() " +
				"ORDER BY deliver_on LIMIT ?";

			final String UPDATE = "UPDATE queue_message " +
				"SET popped = true, popped_on = now(), unack_on = ?, version = version + 1 " +
				"WHERE id = ? AND version = ?";

			while (foundIds.size() < count && ((System.currentTimeMillis() - start) < timeout)) {

				// Get the list of popped message ids
				List<String> popped = queryWithTransaction(QUERY, q -> q.addParameter(queueName.toLowerCase()).addParameter(count)
					.executeAndFetch(rs -> {
						List<String> ids = new LinkedList<>();
						while (rs.next()) {
							long id = rs.getLong("id");
							long version = rs.getLong("version");
							String message_id = rs.getString("message_id");

							withTransaction(connection -> {
								long unack_on = System.currentTimeMillis() + UNACK_TIME_MS;

								int updated = query(connection, UPDATE, u -> u.addTimestampParameter(unack_on)
									.addParameter(id).addParameter(version).executeUpdate());

								// Means record being updated - we got it
								if (updated > 0) {
									ids.add(message_id);
								}
							});
						}
						return ids;
					}));

				if (CollectionUtils.isNotEmpty(popped))
					foundIds.addAll(popped);

				sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
			}

			return Lists.newArrayList(foundIds);
		} catch (Exception ex) {
			logger.error("pop: failed for {} with {}", queueName, ex.getMessage(), ex);
		}

		return Collections.emptyList();
	}

	@Override
	public void processUnacks(String queueName) {
		long unack_on = System.currentTimeMillis() - stalePeriod;

		final String SQL = "UPDATE queue_message " +
			"SET popped = false, deliver_on = now(), popped_on = null, unack_on = null, version = version + 1 " +
			"WHERE queue_name = ? AND popped = true AND unack_on < ?";

		executeWithTransaction(SQL, q -> q.addParameter(queueName.toLowerCase()).addTimestampParameter(unack_on).executeUpdate());
	}

	@Override
	public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
		long unack_on = System.currentTimeMillis() + unackTimeout;

		final String UPDATE = "UPDATE queue_message " +
			"SET popped = true, popped_on = now(), unack_on = ?, version = version + 1 " +
			"WHERE queue_name = ? AND message_id = ?";

		return queryWithTransaction(UPDATE,
			q -> q.addTimestampParameter(unack_on)
				.addParameter(queueName.toLowerCase())
				.addParameter(messageId)
				.executeUpdate()) == 1;
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
					m.popped_on = rs.getTimestamp("popped_on");
					m.unack_on = rs.getTimestamp("unack_on");

					return m;
				}
				return null;
			}));

		if (record == null) {
			pushIfNotExists(queueName, id, 0);
			return false;
		}

		// This method used on conjunction with checking of sweeper queue (see workflow executor)
		// If no record in sweeper queue then this method will be invoked to wake up sweeper for this record
		// But at this time, the record might already been pulled. Tiny moment, but possible to happen
		// So, need to check poppedOn + threshold period.
		if (record.popped_on == null) {
			record.popped_on = new Timestamp(System.currentTimeMillis());
		}

		// If the record pulled within threshold period - do nothing as it might be in sweeper right now
		if (System.currentTimeMillis() - record.popped_on.getTime() < POPPED_THRESHOLD) {
			return false;
		}

		// Otherwise make it visible right away
		final String UPDATE = "UPDATE queue_message " +
			"SET popped = false, deliver_on = now(), popped_on = null, unack_on = null, version = version + 1 " +
			"WHERE id = ? AND version = ?";

		return queryWithTransaction(UPDATE, q -> q.addParameter(record.id)
			.addParameter(record.version).executeUpdate()) > 0;
	}

	private boolean existsMessage(Connection connection, String queueName, String messageId) {
		final String SQL = "SELECT true FROM queue_message WHERE queue_name = ? AND message_id = ?";
		return query(connection, SQL, q -> q.addParameter(queueName.toLowerCase()).addParameter(messageId).exists());
	}

	private void pushMessage(Connection connection, String queueName, String messageId, String payload, long offsetSeconds) {
		createQueueIfNotExists(queueName);

		String SQL = "INSERT INTO queue_message (queue_name, message_id, deliver_on, payload) VALUES (?, ?, ?, ?) " +
			"ON CONFLICT ON CONSTRAINT queue_name_msg DO NOTHING";

		long deliverOn = System.currentTimeMillis() + (offsetSeconds * 1000);

		execute(connection, SQL, q -> q.addParameter(queueName.toLowerCase())
			.addParameter(messageId)
			.addTimestampParameter(deliverOn)
			.addParameter(payload)
			.executeUpdate());
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
		queues.forEach(this::processUnacks);
	}

	private static class QueueMessage {
		long id;
		long version;
		String queue_name;
		String message_id;
		String payload;
		boolean popped;
		Timestamp deliver_on;
		Timestamp popped_on;
		Timestamp unack_on;
	}
}
