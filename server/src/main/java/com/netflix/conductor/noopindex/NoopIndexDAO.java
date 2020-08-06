/*
 * Copyright 2020 Medallia, Inc.
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
package com.netflix.conductor.noopindex;

import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.IndexDAO;

import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Dummy implementation of {@link IndexDAO} which does nothing. Nothing is ever indexed, and no results are ever
 * returned.
 */
@Trace
@Singleton
public class NoopIndexDAO implements IndexDAO {
	@Override
	public void setup() {
	}

	@Override
	public void indexWorkflow(Workflow workflow) {
	}

	@Override
	public CompletableFuture<Void> asyncIndexWorkflow(Workflow workflow) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void indexTask(Task task) {

	}

	@Override
	public CompletableFuture<Void> asyncIndexTask(Task task) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public SearchResult<String> searchWorkflows(String query, String freeText, int start, int count, List<String> sort) {
		return new SearchResult<>(0, Collections.emptyList());
	}

	@Override
	public SearchResult<String> searchTasks(String query, String freeText, int start, int count, List<String> sort) {
		return new SearchResult<>(0, Collections.emptyList());
	}

	@Override
	public void removeWorkflow(String workflowId) {

	}

	@Override
	public CompletableFuture<Void> asyncRemoveWorkflow(String workflowId) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void updateWorkflow(String workflowInstanceId, String[] keys, Object[] values) {

	}

	@Override
	public CompletableFuture<Void> asyncUpdateWorkflow(String workflowInstanceId, String[] keys, Object[] values) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public String get(String workflowInstanceId, String key) {
		return null;
	}

	@Override
	public void addTaskExecutionLogs(List<TaskExecLog> logs) {

	}

	@Override
	public CompletableFuture<Void> asyncAddTaskExecutionLogs(List<TaskExecLog> logs) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public List<TaskExecLog> getTaskExecutionLogs(String taskId) {
		return Collections.emptyList();
	}

	@Override
	public void addEventExecution(EventExecution eventExecution) {

	}

	@Override
	public List<EventExecution> getEventExecutions(String event) {
		return Collections.emptyList();
	}

	@Override
	public CompletableFuture<Void> asyncAddEventExecution(EventExecution eventExecution) {
		return null;
	}

	@Override
	public void addMessage(String queue, Message msg) {

	}

	@Override
	public CompletableFuture<Void> asyncAddMessage(String queue, Message message) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public List<Message> getMessages(String queue) {
		return Collections.emptyList();
	}

	@Override
	public List<String> searchArchivableWorkflows(String indexName, long archiveTtlDays) {
		return Collections.emptyList();
	}

	@Override
	public List<String> searchRecentRunningWorkflows(int lastModifiedHoursAgoFrom, int lastModifiedHoursAgoTo) {
		return Collections.emptyList();
	}
}
