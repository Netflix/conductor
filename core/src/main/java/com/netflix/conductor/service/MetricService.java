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

	public void systemWorkersQueueFull(String taskType) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.system.workers.queue.full");
		tags.add("task_type:" + taskType);
		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void queuePop(String queueName) {
		Set<String> tagsCounter = new HashSet<>();
		tagsCounter.add("metric:deluxe.conductor.queue.poll");
		tagsCounter.add("queue_name:" + queueName);
		statsd.incrementCounter(aspect, toArray(tagsCounter));
	}

	public void taskLockFailed(String taskType) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.task.lock.failed");
		tags.add("task_type:" + taskType);
		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void taskPoll(String taskType, String workerId, int count) {
		Set<String> tagsCounter = new HashSet<>();
		tagsCounter.add("metric:deluxe.conductor.task.poll");
		tagsCounter.add("task_type:" + taskType);
		tagsCounter.add("worker:" + workerId);
		statsd.incrementCounter(aspect, toArray(tagsCounter));

		Set<String> tagsGauge = new HashSet<>();
		tagsGauge.add("metric:deluxe.conductor.task.poll.gauge");
		tagsGauge.add("task_type:" + taskType);
		tagsGauge.add("worker:" + workerId);
		statsd.recordGaugeValue(aspect, count, toArray(tagsGauge));
	}

	public void serviceDiscovery(String serviceName, Long time) {
		Set<String> tagsCounter = new HashSet<>();
		tagsCounter.add("metric:deluxe.conductor.service.discovery");
		tagsCounter.add("service:" + serviceName);
		statsd.incrementCounter(aspect, toArray(tagsCounter));

		Set<String> tagsTimer = new HashSet<>();
		tagsTimer.add("metric:deluxe.conductor.service.discovery.time");
		tagsTimer.add("service:" + serviceName);
		statsd.recordExecutionTime(aspect, time, toArray(tagsTimer));
	}

	public void taskWait(String taskType, String refName, Long waitTime) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.task.queue.wait.time");
		tags.add("task_type:" + taskType);
		tags.add("ref_name:" + refName);
		statsd.recordExecutionTime(aspect, waitTime, toArray(tags));
	}

	public void taskTimeout(String taskType, String refName) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.task.timeout");
		tags.add("task_type:" + taskType);
		tags.add("ref_name:" + refName);
		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void taskRateLimited(String taskType, String refName) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.task.rate.limit");
		tags.add("task_type:" + taskType);
		tags.add("ref_name:" + refName);
		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void taskComplete(String taskType, String refName, String status, long startTime) {
		Set<String> tagsCounter = new HashSet<>();
		tagsCounter.add("metric:deluxe.conductor.task.complete");
		tagsCounter.add("task_type:" + taskType);
		tagsCounter.add("ref_name:" + refName);
		tagsCounter.add("status:" + status);
		statsd.incrementCounter(aspect, toArray(tagsCounter));

		Set<String> tagsTime = new HashSet<>();
		tagsTime.add("metric:deluxe.conductor.task.complete.time");
		tagsTime.add("task_type:" + taskType);
		tagsTime.add("ref_name:" + refName);
		long execTime = System.currentTimeMillis() - startTime;
		statsd.recordExecutionTime(aspect, execTime, toArray(tagsTime));
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

	public void eventExecutionFailed(String handler, String subject) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.event.execution.failed");
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

	public void eventRedeliveryRequested(String handler, String subject) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.event.redelivery.requested");
		tags.add("handler:" + handler);
		tags.add("subject:" + subject);
		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void eventActionExecuted(String handler, String subject, String actionId, long execTime) {
		Set<String> tagsCounter = new HashSet<>();
		tagsCounter.add("metric:deluxe.conductor.event.action.executed");
		tagsCounter.add("handler:" + handler);
		tagsCounter.add("subject:" + subject);
		tagsCounter.add("action:" + actionId);
		statsd.incrementCounter(aspect, toArray(tagsCounter));

		Set<String> tagsTime = new HashSet<>();
		tagsTime.add("metric:deluxe.conductor.event.action.executed.time");
		tagsTime.add("handler:" + handler);
		tagsTime.add("subject:" + subject);
		tagsTime.add("action:" + actionId);
		statsd.recordExecutionTime(aspect, execTime, toArray(tagsTime));
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

	public void workflowPause(String name) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.pause");
		tags.add("workflow:" + name);
		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowResume(String name) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.resume");
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

	public void workflowRemove(String name) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.remove");
		tags.add("workflow:" + name);
		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowRestart(String name) {
		Set<String> tags = new HashSet<>();
		tags.add("metric:deluxe.conductor.workflow.restart");
		tags.add("workflow:" + name);
		statsd.incrementCounter(aspect, toArray(tags));
	}

	public void workflowFailure(String name, String status, long startTime) {
		Set<String> tagsCounter = new HashSet<>();
		tagsCounter.add("metric:deluxe.conductor.workflow.failure");
		tagsCounter.add("workflow:" + name);
		tagsCounter.add("status:" + status);
		statsd.incrementCounter(aspect, toArray(tagsCounter));

		Set<String> tagsTime = new HashSet<>();
		tagsTime.add("metric:deluxe.conductor.workflow.failure.time");
		tagsTime.add("workflow:" + name);
		long execTime = System.currentTimeMillis() - startTime;
		statsd.recordExecutionTime(aspect, execTime, toArray(tagsTime));
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
		Set<String> tagsCounter = new HashSet<>();
		tagsCounter.add("metric:deluxe.conductor.workflow.complete");
		tagsCounter.add("workflow:" + name);
		statsd.incrementCounter(aspect, toArray(tagsCounter));

		Set<String> tagsTime = new HashSet<>();
		tagsTime.add("metric:deluxe.conductor.workflow.complete.time");
		tagsTime.add("workflow:" + name);
		long execTime = System.currentTimeMillis() - startTime;
		statsd.recordExecutionTime(aspect, execTime, toArray(tagsTime));
	}

	public void queueGauge(String queue, Long count) {
		Set<String> tagsCounter = new HashSet<>();
		tagsCounter.add("metric:deluxe.conductor.queue.count");
		tagsCounter.add("queue:" + queue);
		statsd.incrementCounter(aspect, toArray(tagsCounter));
		
		Set<String> tagsGauge = new HashSet<>();
		tagsGauge.add("metric:deluxe.conductor.queue.gauge");
		tagsGauge.add("queue:" + queue);
		statsd.recordGaugeValue(aspect, count, toArray(tagsGauge));
	}

	public void workflowGauge(String workflow, Long count) {
		Set<String> tagsCounter = new HashSet<>();
		tagsCounter.add("metric:deluxe.conductor.workflow.running.count");
		tagsCounter.add("workflow:" + workflow);
		statsd.incrementCounter(aspect, toArray(tagsCounter));

		Set<String> tagsGauge = new HashSet<>();
		tagsGauge.add("metric:deluxe.conductor.workflow.running.gauge");
		tagsGauge.add("workflow:" + workflow);
		statsd.recordGaugeValue(aspect, count, toArray(tagsGauge));
	}
}
