/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.server.resources;

import com.google.common.collect.ImmutableMap;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.histogram.PercentileTimer;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author Oleksiy Lysak
 *
 */
@Singleton
@Path("/v1")
@Api(value = "/v1", produces = MediaType.APPLICATION_JSON, tags = "Status Info")
@Produces({MediaType.APPLICATION_JSON})
public class InfoResource {
	private static Logger logger = LoggerFactory.getLogger(InfoResource.class);
	private String fullVersion;
	private Configuration config;

	@Inject
	public InfoResource(Configuration config) {
		this.config = config;
		try {
			InputStream propertiesIs = this.getClass().getClassLoader().getResourceAsStream("META-INF/conductor-core.properties");
			Properties prop = new Properties();
			prop.load(propertiesIs);
			String version = prop.getProperty("Implementation-Version");
			String change = prop.getProperty("Change");
			fullVersion = config.getProperty("APP.VERSION", version + "-" + change);
		} catch (Exception e) {
			logger.error("Failed to read conductor-core.properties" + e.getMessage(), e);
		}
	}

	@GET
	@Path("/status")
	@ApiOperation(value = "Get the status")
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String, Object> status() {
		return Collections.singletonMap("version", fullVersion);
	}

	@GET
	@Path("/dependencies")
	@ApiOperation(value = "Get the dependencies")
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String, Object> dependencies() {
		List<Object> endpoints = new ArrayList<>();
		endpoints.add(config.getProperty("conductor.auth.url", ""));
		endpoints.add("events.service." + config.getProperty("TLD", "local"));
		endpoints.add("vault.service." + config.getProperty("TLD", "local"));

		List<Map<String, Object>> dependencies = new ArrayList<>();
		dependencies.add(ImmutableMap.<String, Object>builder()
				.put("name", "auth")
				.put("version", "v1")
				.put("scheme", "https")
				.put("external", false)
				.build());
		dependencies.add(ImmutableMap.<String, Object>builder()
				.put("name", "vault")
				.put("version", "v1")
				.put("scheme", "http")
				.put("external", false)
				.build());
		dependencies.add(ImmutableMap.<String, Object>builder()
				.put("name", "events")
				.put("version", "v1")
				.put("scheme", "nats")
				.put("external", false)
				.build());
		dependencies.add(ImmutableMap.<String, Object>builder()
				.put("name", "*")
				.put("version", "v1")
				.put("scheme", "http")
				.put("external", false)
				.build());

		Map<String, Object> result = new HashMap<>();
		result.put("version", fullVersion);
		result.put("endpoints", endpoints);
		result.put("dependencies", dependencies);
		return result;
	}

	@GET
	@Path("/metrics")
	@ApiOperation(value = "Get the metrics")
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String, Object> metrics() {
		Map<String, Object> output = new TreeMap<>();
		final String prefix = "deluxe.conductor.";

		// Workflow and Event Counters
		Map<String, String> counterMap = new HashMap<>();
		Map<String, String> todayMap = new HashMap<>();

		// Map counter names to metric names
		counterMap.put("workflow_completion", "workflows_completed");
		counterMap.put("workflow_failure", "workflows_failed");
		counterMap.put("workflow_start", "workflows_started");
		counterMap.put("workflow_cancel", "workflows_canceled");
		counterMap.put("workflow_restart", "workflows_restarted");
		todayMap.put("workflow_completion", "workflows_completed_today");
		todayMap.put("workflow_start", "workflows_started_today");

		// Messages
		counterMap.put("event_queue_messages_received", "messages_received");
		counterMap.put("event_queue_messages_processed", "messages_processed");

		// Counters
		final Map<String, Map<Map<String, String>, Counter>> counters = Monitors.getCounters();
		final LocalDate today = LocalDate.now();
		counters.forEach((name, map) -> {
			if (counterMap.containsKey(name)) {
				output.put(prefix + counterMap.get(name), sum(map));
			}

			// Workflows Started or Completed Today
			if (todayMap.containsKey(name)) {
				final long sum = map.entrySet().stream()
					.filter(e -> e.getKey().containsKey("date"))
					.mapToLong(e -> {
						final LocalDate value = LocalDate.parse(e.getKey().get("date"));
						return today.isEqual(value) ? e.getValue().count() : 0;
					})
					.sum();
				output.put(prefix + todayMap.get(name), sum);
			}
		});


		// Gauges to track in progress tasks, workflows, etc...
		Map<String, String> gaugeMap = new HashMap<>();

		// Map gauge names to metric names
		gaugeMap.put("workflow_in_progress", "workflows_in_progress");
		gaugeMap.put("task_in_progress", "tasks_in_progress");

		// Output gauges
		final Map<String, Map<Map<String, String>, AtomicLong>> gauges = Monitors.getGauges();
		gauges.forEach((name, map) -> {
			if (gaugeMap.containsKey(name)) {
				final long value = map.values().stream().mapToLong(v -> v.get()).sum();
				output.put(prefix + gaugeMap.get(name), value);
			}
		});


		// Timers
		Map<String, String> timerMap = new HashMap<>();

		// Map timer names to metric names
		timerMap.put("task_queue_wait", "avg_task_queue_wait_ms");

		final Map<String, Map<Map<String, String>, PercentileTimer>> timers = Monitors.getTimers();

		// Basic timers
		timers.forEach((name, map) -> {
			map.forEach((tags, timer) -> {
				if (timerMap.containsKey(name)) {
					final long avgMs = Duration.ofNanos(timer.totalTime()).toMillis() / timer.count();
					output.put(prefix + timerMap.get(name), avgMs);
				}
			});
		});

		// Average Execution Times
		Map<String, String> executionMap = new HashMap<>();

		executionMap.put("task_execution", "avg_task_execution_ms");
		executionMap.put("workflow_execution", "avg_workflow_execution_ms");

		timers.forEach((name, map) -> {
			if (executionMap.containsKey(name)) {
				long totalCount = 0;
				long totalTime = 0;

				Iterator<Map.Entry<Map<String, String>, PercentileTimer>> iterator = map.entrySet().iterator();
				while (iterator.hasNext()) {
					Map.Entry<Map<String, String>, PercentileTimer> entry = iterator.next();
					PercentileTimer timer = entry.getValue();

					totalCount += timer.count();
					totalTime += Duration.ofNanos(timer.totalTime()).toMillis();
				}
				
				output.put(prefix + executionMap.get(name), totalTime / totalCount);
			}
		});

		return output;
	}

	@GET
	@Path("/metrics/all")
	@ApiOperation(value = "Get all available metrics")
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String, Object> debugMetrics() {
		Map<String, Object> output = new TreeMap<>();

		output.putAll(getCounters());
		output.putAll(getGauges());
		output.putAll(getTimers());

		return output;
	}

	@GET
	@Path("/metrics/counters")
	@ApiOperation(value = "Get the counter metrics")
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String, Object> counters() {
		return new TreeMap<>(getCounters());
	}

	@GET
	@Path("/metrics/gauges")
	@ApiOperation(value = "Get the gauge metrics")
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String, Object> gauges() {
		return new TreeMap<>(getGauges());
	}

	@GET
	@Path("/metrics/timers")
	@ApiOperation(value = "Get the timer metrics")
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String, Object> timers() {
		return new TreeMap<>(getTimers());
	}

	private Map<String, Object> getCounters() {
		Map<String, Object> output = new HashMap<>();

		final Map<String, Map<Map<String, String>, Counter>> counters = Monitors.getCounters();
		counters.forEach((name, map) -> {
			map.forEach((tags, counter) -> {
				output.put(name + "." + joinTags(tags) + ".counter", counter.count());
			});
		});

		return output;
	}

	private Map<String, Object> getGauges() {
		Map<String, Object> output = new HashMap<>();

		final Map<String, Map<Map<String, String>, AtomicLong>> gauges = Monitors.getGauges();
		gauges.forEach((name, map) -> {
			map.forEach((tags, value) -> {
				output.put(name + "." + joinTags(tags) + ".value", value.get());
			});
		});

		return output;
	}

	private Map<String, Object> getTimers() {
		Map<String, Object> output = new HashMap<>();

		final Map<String, Map<Map<String, String>, PercentileTimer>> timers = Monitors.getTimers();
		timers.forEach((name, map) -> {
			map.forEach((tags, timer) -> {
				String key = joinTags(tags);
				output.put(name + "." + key + ".count", timer.count());
				output.put(name + "." + key + ".totalTime", timer.totalTime());
			});
		});

		return output;
	}

	private String joinTags(Map<String, String> tags) {
		// Concatenate all tags into single line: tag1.tag2.tagX excluding class name
		return tags.entrySet().stream()
			.filter(entry -> !entry.getKey().equals("class"))
			.map(Map.Entry::getValue).collect(Collectors.joining("."));
	}

	// Return the sum of the Counter values in m
	private long sum(Map<Map<String, String>, Counter> m) {
		return m.values().stream().map(c -> {return c.count();}).mapToLong(i -> i).sum();
	}
}
