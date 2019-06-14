package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.IndexDAO;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class AuroraIndexDAO extends AuroraBaseDAO implements IndexDAO {

	@Inject
	public AuroraIndexDAO(DataSource dataSource, ObjectMapper mapper) {
		super(dataSource, mapper);
	}

	@Override
	public void index(Workflow workflow) {
		// Do nothing here
	}

	@Override
	public void index(Task task) {
		// Do nothing here
	}

	@Override
	public void remove(String workflowId) {
		// Do nothing here
	}

	@Override
	public void update(String workflowInstanceId, String[] keys, Object[] values) {
		// Do nothing here
	}

	@Override
	public String get(String workflowInstanceId, String key) {
		// Do nothing here
		return null;
	}

	@Override
	public List<TaskExecLog> getTaskLogs(String taskId) {
		String SQL = "SELECT created_on, log FROM task_log WHERE task_id = ?";

		return queryWithTransaction(SQL, q -> q.addParameter(taskId).executeAndFetch(rs -> {
			List<TaskExecLog> logs = new LinkedList<>();
			while (rs.next()) {
				TaskExecLog log = new TaskExecLog();

				log.setTaskId(taskId);
				log.setCreatedTime(rs.getTimestamp("created_on").getTime());
				log.setLog(rs.getString("log"));

				logs.add(log);
			}

			return logs;
		}));
	}

	@Override
	public void add(List<TaskExecLog> logs) {
		if (logs == null || logs.isEmpty()) {
			return;
		}

		String SQL = "INSERT INTO task_log(created_on, task_id, log) VALUES (?, ?, ?)";

		withTransaction(connection -> {
			for (TaskExecLog log : logs) {
				execute(connection, SQL,
					q -> q.addTimestampParameter(log.getCreatedTime(), System.currentTimeMillis())
						.addParameter(log.getTaskId())
						.addParameter(log.getLog())
						.executeUpdate());
			}
		});
	}

	@Override
	public void add(EventExecution ee) {
		// Do nothing here as it is handled by AuroraExecutionDAO
	}

	@Override
	public void addMessage(String queue, Message msg) {
		String SQL = "INSERT INTO event_message(queue_name, message_id, receipt, json_data) " +
			"VALUES (?, ?, ?, ?)";

		withTransaction(connection -> {
			execute(connection, SQL,
				q -> q.addParameter(queue)
					.addParameter(msg.getId())
					.addParameter(msg.getReceipt())
					.addParameter(msg.getPayload())
					.executeUpdate());
		});
	}

	@Override
	@SuppressWarnings("unchecked")
	public SearchResult<String> searchWorkflows(String query, String freeText, int start, int count, List<String> sort, String from, String end) {
		logger.debug("searchWorkflows with query=" + query + ", freeText=" + freeText + ", start=" + start +
			", count=" + count + ", sort=" + sort + ", from=" + from + ", end=" + end);

		// Where statement here only to make simple the parseQuery/parseFreeText so they always can use AND ... AND ...
		StringBuilder SQL = new StringBuilder("SELECT count(*) OVER() AS total_count, workflow_id FROM workflow WHERE 1=1 ");

		LinkedList<Object> params = new LinkedList<>();
		parseQuery(query, SQL, params);
		parseFreeText(freeText, SQL, params);

		SQL.append("ORDER BY start_time DESC LIMIT ? OFFSET ?");

		return queryWithTransaction(SQL.toString(), q -> {
			params.forEach(p -> {
				if (p instanceof Timestamp) {
					q.addParameter((Timestamp)p);
				} else if (p instanceof List) {
					q.addParameter((Collection<String>) p);
				} else if (p instanceof String) {
					q.addParameter((String) p);
				}
			});

			q.addParameter(count); // limit
			q.addParameter(start); // offset

			return q.executeAndFetch(rs -> {
				long totalHits = 0;
				List<String> ids = new LinkedList<>();
				while (rs.next()) {
					if (totalHits == 0) {
						totalHits = rs.getLong("total_count");
					}
					ids.add(rs.getString("workflow_id"));
				}
				return new SearchResult<>(totalHits, ids);
			});
		});
	}

	// Parse query: workflowType IN (deluxe.dependencygraph.source_wait.sherlock.1.0,deluxe.atlas.createasset.process.1.0)  AND status IN (RUNNING,FAILED)
	private void parseQuery(String query, StringBuilder SQL, LinkedList<Object> params) {
		if (StringUtils.isEmpty(query)) {
			return;
		}

		String[] ands = query.split("AND");
		if (ArrayUtils.isEmpty(ands)) {
			return;
		}

		Arrays.stream(ands).map(String::trim).forEach(s -> {
			int start = s.indexOf("(");
			int finish = s.indexOf(")");

			String[] strings = s.substring(start + 1, finish).split(",");
			List<String> values = Arrays.asList(strings);

			if (s.startsWith("workflowType IN")) {
				SQL.append("AND workflow_type = ANY (?) ");
				params.add(values);
			} else if (s.startsWith("status IN")) {
				SQL.append("AND workflow_status = ANY (?) ");
				params.add(values);
			}
		});
	}

	// Parse freeText: * AND startTime:[2019-06-07T00:00:00.000Z TO 2019-06-07T23:59:59.999Z]
	// Parse freeText: bla-bla-bla AND startTime:[2019-06-07T00:00:00.000Z TO 2019-06-07T23:59:59.999Z]
	private void parseFreeText(String freeText, StringBuilder SQL, LinkedList<Object> params) {
		if (StringUtils.isEmpty(freeText)) {
			return;
		}

		String[] ands = freeText.split("AND");
		if (ArrayUtils.isEmpty(ands)) {
			return;
		}

		Arrays.stream(ands).map(String::trim).forEach(s -> {
			if (s.startsWith("startTime:")) {
				int start = s.indexOf("[");
				int finish = s.indexOf("]");

				String[] strings = s.substring(start + 1, finish).split(" TO ");

				DateTime from = ISODateTimeFormat.dateTime().parseDateTime(strings[0]);
				DateTime to = ISODateTimeFormat.dateTime().parseDateTime(strings[1]);

				SQL.append("AND start_time BETWEEN ? AND ? ");
				params.add(new Timestamp(from.getMillis()));
				params.add(new Timestamp(to.getMillis()));
			} else if (!s.equals("*")) { // Do none filtering "*", just to match UI expectation to fetch all data
				// Otherwise apply filter by data
				SQL.append("AND json_data LIKE ?");
				params.add("%" + s + "%");
			}
		});
	}
}
