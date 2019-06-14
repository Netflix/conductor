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
package com.netflix.conductor.dao;

import java.util.Map;

/**
 * @author Oleksiy Lysak
 */
public interface MetricsDAO {
	boolean ping();

	Map<String, Object> getMetrics();

	Map<String, Object> getAdminCounters();

	Map<String, Object> getEventReceived();

	Map<String, Object> getEventPublished();

	Map<String, Object> getEventExecAverage();

	Map<String, Object> getEventWaitAverage();

	Map<String, Object> getTaskCounters();

	Map<String, Object> getTaskAverage();

	Map<String, Object> getWorkflowCounters();

	Map<String, Object> getWorkflowAverage();
}
