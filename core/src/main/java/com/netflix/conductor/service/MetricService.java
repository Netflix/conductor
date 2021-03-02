package com.netflix.conductor.service;

import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class MetricService {
	private static volatile MetricService instance = null;
	private final String aspect = "service.metric";
	private final StatsDClient statsd;

	public static MetricService getInstance() {
		if (instance != null) {
			return instance;
		}
		synchronized (MetricService.class) {
			if (instance == null) {
				instance = new MetricService();
			}
		}
		return instance;
	}

	private MetricService() {
		String container = getHostName();
		String stack = System.getenv("STACK");
		String allocId = System.getenv("NOMAD_ALLOC_ID");

		String tld = System.getenv("TLD");
		statsd = new NonBlockingStatsDClientBuilder()
			.constantTags("environment:" + stack, "container:" + container, "alloc_id:" + allocId)
			.hostname("datadog.service." + tld)
			.port(8125)
			.build();
	}

	private String getHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return "unknown";
		}
	}

	private String[] toArray(Collection<String> tags) {
		return tags.toArray(new String[0]);
	}

	public void serverStarted() {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.server.started");

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void taskPoll(String taskType, String workerId) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.task.poll");
		tags.add("task_type:" + taskType);
		tags.add("worker:" + workerId);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void taskWait(String taskType, Long waitTime) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.task.queue.wait");
		tags.add("task_type:" + taskType);

		statsd.recordExecutionTime(aspect, waitTime, toArray(tags));
	}

	public void eventReceived(String subject) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.event.received");
		tags.add("subject:" + subject);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void eventPublished(String subject) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.event.published");
		tags.add("subject:" + subject);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void eventExecutionSkipped(String handler, String subject) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.event.execution.skipped");
		tags.add("handler:" + handler);
		tags.add("subject:" + subject);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void eventActionSkipped(String handler, String subject, String actionId) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.event.action.skipped");
		tags.add("handler:" + handler);
		tags.add("subject:" + subject);
		tags.add("action:" + actionId);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void eventActionExecuted(String handler, String subject, String actionId, long execTime) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.event.action.executed");
		tags.add("handler:" + handler);
		tags.add("subject:" + subject);
		tags.add("action:" + actionId);

		statsd.incrementCounter(aspect, toArray(tags));
		statsd.recordExecutionTime(aspect, execTime, toArray(tags));
	}

	public void eventActionFailed(String handler, String subject, String actionId) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.event.action.failed");
		tags.add("handler:" + handler);
		tags.add("subject:" + subject);
		tags.add("action:" + actionId);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void eventTagsHit(String handler, String subject) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.event.tags.hit");
		tags.add("handler:" + handler);
		tags.add("subject:" + subject);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void eventTagsMiss(String handler, String subject) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.event.tags.miss");
		tags.add("handler:" + handler);
		tags.add("subject:" + subject);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowStart(String name) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.start");
		tags.add("workflow:" + name);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowStartFailed(String name) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.start.failed");
		tags.add("workflow:" + name);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowRerun(String name) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.rerun");
		tags.add("workflow:" + name);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowReset(String name) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.reset");
		tags.add("workflow:" + name);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowRetry(String name) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.retry");
		tags.add("workflow:" + name);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowRestart(String name) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.restart");
		tags.add("workflow:" + name);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowFailure(String name, String status) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.failure");
		tags.add("workflow:" + name);
		tags.add("status:" + status);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowCancel(String name) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.cancel");
		tags.add("workflow:" + name);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowForceComplete(String name) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.force.complete");
		tags.add("workflow:" + name);

		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowComplete(String name, long startTime) {
		long execTime = System.currentTimeMillis() - startTime;
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.complete");
		tags.add("workflow:" + name);

		statsd.incrementCounter(aspect, toArray(tags));
		statsd.recordExecutionTime(aspect, execTime, toArray(tags));
	}

	public void queueGauge(String queue, Long count) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.queue.count");
		tags.add("queue:" + queue);

		statsd.incrementCounter(aspect, toArray(tags));
		statsd.recordGaugeValue(aspect, count, toArray(tags));
	}

	public void workflowGauge(String workflow, Long count) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.running.count");
		tags.add("workflow:" + workflow);

		statsd.incrementCounter(aspect, toArray(tags));
		statsd.recordGaugeValue(aspect, count, toArray(tags));
	}
}
