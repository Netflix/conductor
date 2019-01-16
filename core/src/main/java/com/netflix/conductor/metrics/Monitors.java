/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.metrics;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.Workflow.WorkflowStatus;
import com.netflix.servo.monitor.BasicStopwatch;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.spectator.api.*;
import com.netflix.spectator.api.histogram.PercentileTimer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Viren
 *
 */
public class Monitors {

	private static Registry registry = Spectator.globalRegistry();

	private static Map<String, Map<Map<String, String>, Counter>> counters = new ConcurrentHashMap<>();

	private static Map<String, Map<Map<String, String>, PercentileTimer>> timers = new ConcurrentHashMap<>();

	private static Map<String, Map<Map<String, String>, AtomicLong>> gauges = new ConcurrentHashMap<>();

	public static final String classQualifier = "WorkflowMonitor";

	private Monitors() {

	}

	/**
	 *
	 * @param className Name of the class
	 * @param methodName Method name
	 *
	 */
	public static void error(String className, String methodName) {
		getCounter(className, "workflow_server_error", "methodName", methodName).increment();
	}

	public static Stopwatch start(String className, String name, String... additionalTags) {
		return start(getTimer(className, name, additionalTags));
	}

	public static Map<String, Map<Map<String, String>, Counter>> getCounters() {
		return counters;
	}

	public static Map<String, Map<Map<String, String>, AtomicLong>> getGauges() {
		return gauges;
	}

	public static Map<String, Map<Map<String, String>, PercentileTimer>> getTimers() {
		return timers;
	}

	/**
	 * Increment a counter that is used to measure the rate at which some event
	 * is occurring. Consider a simple queue, counters would be used to measure
	 * things like the rate at which items are being inserted and removed.
	 *
	 * @param className
	 * @param name
	 * @param additionalTags
	 */
	private static void counter(String className, String name, String... additionalTags) {
		getCounter(className, name, additionalTags).increment();
	}

	/**
	 * Set a gauge is a handle to get the current value. Typical examples for
	 * gauges would be the size of a queue or number of threads in the running
	 * state. Since gauges are sampled, there is no information about what might
	 * have occurred between samples.
	 *
	 * @param className
	 * @param name
	 * @param measurement
	 * @param additionalTags
	 */
	private static void gauge(String className, String name, long measurement, String... additionalTags) {
		getGauge(className, name, additionalTags).getAndSet(measurement);
	}

	public static Timer getTimer(String className, String name, String... additionalTags) {
		Map<String, String> tags = toMap(className, additionalTags);
		tags.put("unit", TimeUnit.SECONDS.name());
		return timers.computeIfAbsent(name, s -> new ConcurrentHashMap<>()).computeIfAbsent(tags, t -> {
			Id id = registry.createId(name, tags);
			return PercentileTimer.get(registry, id);
		});
	}

	private static Counter getCounter(String className, String name, String... additionalTags) {
		Map<String, String> tags = toMap(className, additionalTags);

		return counters.computeIfAbsent(name, s -> new ConcurrentHashMap<>()).computeIfAbsent(tags, t -> {
			Id id = registry.createId(name, tags);
			return registry.counter(id);
		});
	}

	private static AtomicLong getGauge(String className, String name, String... additionalTags) {
		Map<String, String> tags = toMap(className, additionalTags);

		return gauges.computeIfAbsent(name, s -> new ConcurrentHashMap<>()).computeIfAbsent(tags, t -> {
			Id id = registry.createId(name, tags);
			return registry.gauge(id, new AtomicLong(0));
		});
	}

	private static Map<String, String> toMap(String className, String... additionalTags) {
		Map<String, String> tags = new HashMap<>();
		tags.put("class", className);
		for (int j = 0; j < additionalTags.length - 1; j++) {
			String tk = additionalTags[j];
			String tv = "" + additionalTags[j + 1];
			if(!tv.isEmpty()) {
				tags.put(tk, tv);
			}
			j++;
		}
		return tags;
	}

	private static Stopwatch start(Timer sm) {

		Stopwatch sw = new BasicStopwatch() {

			@Override
			public void stop() {
				super.stop();
				long duration = getDuration(TimeUnit.MILLISECONDS);
				sm.record(duration, TimeUnit.MILLISECONDS);
			}

		};
		sw.start();
		return sw;
	}

	public static void recordQueueWaitTime(String taskType, long queueWaitTime) {
		getTimer(classQualifier, "task_queue_wait", "taskType", taskType).record(queueWaitTime, TimeUnit.MILLISECONDS);
	}

	public static void recordTaskExecutionTime(String taskType, long duration, boolean includesRetries, Task.Status status) {
		getTimer(classQualifier, "task_execution", "taskType", taskType, "includeRetries", "" + includesRetries, "status", status.name()).record(duration, TimeUnit.MILLISECONDS);
	}

	public static void recordTaskPoll(String taskType) {
		counter(classQualifier, "task_poll", "taskType", taskType);
	}

	public static void recordQueueDepth(String taskType, long size, String ownerApp) {
		gauge(classQualifier, "task_queue_depth", size, "taskType", taskType, "ownerApp", ""+ownerApp);
	}	

	public static void recordTaskInProgress(String taskType, long size, String ownerApp) {
		gauge(classQualifier, "task_in_progress", size, "taskType", taskType, "ownerApp", ""+ownerApp);
	}

	public static void recordRunningWorkflows(long count, String name, String version, String ownerApp) {
		gauge(classQualifier, "workflow_running", count, "workflowName", name, "version", version, "ownerApp", ""+ownerApp);
	}

	public static void recordWorkflowInProgress(Workflow workflow) {
		if (!workflow.isSubWorkflow()) {
			getGauge(classQualifier, "workflow_in_progress", "workflowName", workflow.getWorkflowType()).getAndIncrement();
		}
	}

	public static void recordWorkflowCompleteProgress(Workflow workflow) {
		if (!workflow.isSubWorkflow()) {
			AtomicLong gauge = getGauge(classQualifier, "workflow_in_progress", "workflowName", workflow.getWorkflowType());
			final long value = gauge.get();

			if (value > 0) {
				gauge.set(value - 1);
			}
		}
	}

	public static void recordTaskTimeout(String taskType) {
		counter(classQualifier, "task_timeout", "taskType", taskType);
	}

	public static void recordTaskResponseTimeout(String taskType) {
		counter(classQualifier, "task_response_timeout", "taskType", taskType);
	}

	public static void recordWorkflowTermination(Workflow workflow) {
		final String name = prefixName("workflow_failure", "sub", workflow.isSubWorkflow());
		counter(classQualifier, name, "workflowName", workflow.getWorkflowType(), "status", workflow.getStatus().name());
		recordWorkflowCompleteProgress(workflow);
	}

	public static void recordWorkflowStartError(String workflowType) {
		counter(classQualifier, "workflow_start_error", "workflowName", workflowType);
	}

	public static void recordUpdateConflict(String taskType, String workflowType, WorkflowStatus status) {
		counter(classQualifier, "task_update_conflict", "workflowName", workflowType, "taskType", taskType, "workflowStatus", status.name());
	}

	public static void recordUpdateConflict(String taskType, String workflowType, Status status) {
		counter(classQualifier, "task_update_conflict", "workflowName", workflowType, "taskType", taskType, "workflowStatus", status.name());
	}

	public static void recordWorkflowCompletion(Workflow workflow) {
		final String type = workflow.getWorkflowType();
		final String name = prefixName("workflow_execution", "sub", workflow.isSubWorkflow());
		getTimer(classQualifier, name, "workflowName", type).record(workflow.getDuration(), TimeUnit.MILLISECONDS);
		recordWorkflowCompletion(type); // counter
		recordWorkflowCompleteProgress(workflow);
	}

	public static void recordWorkflowCompletion(String workflowType) {
		counter(classQualifier, "workflow_completion", "workflowName", workflowType);
	}

	public static void recordTaskRateLimited(String taskDefName, int limit) {
		gauge(classQualifier, "task_rate_limited", limit, "taskType", taskDefName);
	}

	public static void recordEventQueueMessagesProcessed(String queueType, String queueName, int count) {
		getCounter(classQualifier, "event_queue_messages_processed", "queueType", queueType, "queueName", queueName).increment(count);
	}

	public static void recordEventQueueMessagesReceived(String queueType, String queueName) {
		getCounter(classQualifier, "event_queue_messages_received", "queueType", queueType, "queueName", queueName).increment();
	}

	public static void recordObservableQMessageReceivedErrors(String queueType) {
		counter(classQualifier, "observable_queue_error", "queueType", queueType);
	}

	public static void recordDaoRequests(String dao, String action, String taskType, String workflowType) {
		counter(classQualifier, "dao_requests", "dao", dao, "action", action, "taskType", taskType, "workflowType", workflowType);
	}

	public static void recordDaoEventRequests(String dao, String action, String event) {
		counter(classQualifier, "dao_requests", "dao", dao, "action", action, "event", event);
	}

	public static void recordDaoPayloadSize(String dao, String action, int size) {
		gauge(classQualifier, "dao_payload_size", size, "dao", dao, "action", action);
	}

	public static void recordWorkflowStart(Workflow workflow) {
		final String name = prefixName("workflow_start", "sub", workflow.isSubWorkflow());
		counter(classQualifier, name, "workflowName", workflow.getWorkflowType());
		recordWorkflowInProgress(workflow);
	}

	public static void recordWorkflowPause(Workflow workflow) {
		final String name = prefixName("workflow_pause", "sub", workflow.isSubWorkflow());
		counter(classQualifier, name, "workflowName", workflow.getWorkflowType());
	}

	public static void recordWorkflowResume(Workflow workflow) {
		final String name = prefixName("workflow_resume", "sub", workflow.isSubWorkflow());
		counter(classQualifier, name, "workflowName", workflow.getWorkflowType());
	}

	public static void recordWorkflowCancel(Workflow workflow) {
		final String name = prefixName("workflow_cancel", "sub", workflow.isSubWorkflow());
		counter(classQualifier, name, "workflowName", workflow.getWorkflowType());
		recordWorkflowCompleteProgress(workflow);
	}

	public static void recordWorkflowReset(Workflow workflow) {
		final String name = prefixName("workflow_reset", "sub", workflow.isSubWorkflow());
		counter(classQualifier, name, "workflowName", workflow.getWorkflowType());
	}

	public static void recordWorkflowRerun(Workflow workflow) {
		final String name = prefixName("workflow_rerun", "sub", workflow.isSubWorkflow());
		counter(classQualifier, name, "workflowName", workflow.getWorkflowType());
	}

	public static void recordWorkflowRetry(Workflow workflow) {
		final String name = prefixName("workflow_retry", "sub", workflow.isSubWorkflow());
		counter(classQualifier, name, "workflowName", workflow.getWorkflowType());
	}

	public static void recordWorkflowRestart(Workflow workflow) {
		final String name = prefixName("workflow_restart", "sub", workflow.isSubWorkflow());
		counter(classQualifier, name, "workflowName", workflow.getWorkflowType());
		recordWorkflowInProgress(workflow);
	}

	public static void recordWorkflowRemove(Workflow workflow) {
		final String name = prefixName("workflow_remove", "sub", workflow.isSubWorkflow());
		counter(classQualifier, name, "workflowName", workflow.getWorkflowType());
	}

	private static String prefixName(String name, String prefix, boolean condition) {
		if (condition) {
			return prefix + name;
		}

		return name;
	}
}
