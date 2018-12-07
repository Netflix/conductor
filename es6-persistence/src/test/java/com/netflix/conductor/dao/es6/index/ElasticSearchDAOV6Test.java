package com.netflix.conductor.dao.es6.index;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.support.TestRunner;
import com.netflix.conductor.support.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(TestRunner.class)
public class ElasticSearchDAOV6Test {

    @Inject
    private ElasticSearchDAOV6 dao;

    @Test
    public void shouldIndexWorkflow() {
        Workflow workflow = TestUtils.loadWorkflowSnapshot("workflow");
        WorkflowSummary summary = new WorkflowSummary(workflow);

        dao.indexWorkflow(workflow);

        assertWorkflowSummary(workflow, summary);
    }

    @Test
    public void shouldIndexWorkflowAsync() throws Exception {
        Workflow workflow = TestUtils.loadWorkflowSnapshot("workflow");
        WorkflowSummary summary = new WorkflowSummary(workflow);

        dao.asyncIndexWorkflow(workflow).get();

        assertWorkflowSummary(workflow, summary);
    }

    @Test
    public void shouldIndexTask() {
        Workflow workflow = TestUtils.loadWorkflowSnapshot("workflow");
        Task task = workflow.getTasks().get(0);

        TaskSummary summary = new TaskSummary(task);

        dao.indexTask(task);

        List<String> tasks = tryFindResults(() -> getTasks(workflow));

        assertEquals(summary.getTaskId(), tasks.get(0));
    }

    @Test
    public void shouldIndexTaskAsync() throws Exception {
        Workflow workflow = TestUtils.loadWorkflowSnapshot("workflow");
        Task task = workflow.getTasks().get(0);

        TaskSummary summary = new TaskSummary(task);

        dao.asyncIndexTask(task).get();

        List<String> tasks = tryFindResults(() -> getTasks(workflow));

        assertEquals(summary.getTaskId(), tasks.get(0));
    }

    @Test
    public void shouldAddTaskExecutionLogs() {
        List<TaskExecLog> logs = new ArrayList<>();
        String taskId = uuid();
        logs.add(createLog(taskId, "log1"));
        logs.add(createLog(taskId, "log2"));
        logs.add(createLog(taskId, "log3"));

        dao.addTaskExecutionLogs(logs);

        List<TaskExecLog> indexedLogs = tryFindResults(() -> dao.getTaskExecutionLogs(taskId), 3);

        assertEquals(3, indexedLogs.size());

        assertTrue("Not all logs was indexed", indexedLogs.containsAll(logs));
    }

    @Test
    public void shouldAddTaskExecutionLogsAsync() throws Exception {
        List<TaskExecLog> logs = new ArrayList<>();
        String taskId = uuid();
        logs.add(createLog(taskId, "log1"));
        logs.add(createLog(taskId, "log2"));
        logs.add(createLog(taskId, "log3"));

        dao.asyncAddTaskExecutionLogs(logs).get();

        List<TaskExecLog> indexedLogs = tryFindResults(() -> dao.getTaskExecutionLogs(taskId), 3);

        assertEquals(3, indexedLogs.size());

        assertTrue("Not all logs was indexed", indexedLogs.containsAll(logs));
    }

    @Test
    public void shouldAddMessage() {
        String queue = "queue";
        Message message1 = new Message(uuid(), "payload1", null);
        Message message2 = new Message(uuid(), "payload2", null);

        dao.addMessage(queue, message1);
        dao.addMessage(queue, message2);

        List<Message> indexedMessages = tryFindResults(() -> dao.getMessages(queue), 2);

        assertEquals(2, indexedMessages.size());

        assertTrue("Not all messages was indexed", indexedMessages.containsAll(Arrays.asList(message1, message2)));
    }

    @Test
    public void shouldAddEventExecution() {
        String event = "event";
        EventExecution execution1 = createEventExecution(event);
        EventExecution execution2 = createEventExecution(event);

        dao.addEventExecution(execution1);
        dao.addEventExecution(execution2);

        List<EventExecution> indexedExecutions = tryFindResults(() -> dao.getEventExecutions(event), 2);

        assertEquals(2, indexedExecutions.size());

        assertTrue("Not all event executions was indexed", indexedExecutions.containsAll(Arrays.asList(execution1, execution2)));
    }

    @Test
    public void shouldAsyncAddEventExecution() throws Exception {
        String event = "event2";
        EventExecution execution1 = createEventExecution(event);
        EventExecution execution2 = createEventExecution(event);

        dao.asyncAddEventExecution(execution1).get();
        dao.asyncAddEventExecution(execution2).get();

        List<EventExecution> indexedExecutions = tryFindResults(() -> dao.getEventExecutions(event), 2);

        assertEquals(2, indexedExecutions.size());

        assertTrue("Not all event executions was indexed", indexedExecutions.containsAll(Arrays.asList(execution1, execution2)));
    }

    private void assertWorkflowSummary(Workflow workflow, WorkflowSummary summary) {
        assertEquals(summary.getWorkflowType(), dao.get(workflow.getWorkflowId(), "workflowType"));
        assertEquals(String.valueOf(summary.getVersion()), dao.get(workflow.getWorkflowId(), "version"));
        assertEquals(summary.getWorkflowId(), dao.get(workflow.getWorkflowId(), "workflowId"));
        assertEquals(summary.getCorrelationId(), dao.get(workflow.getWorkflowId(), "correlationId"));
        assertEquals(summary.getStartTime(), dao.get(workflow.getWorkflowId(), "startTime"));
        assertEquals(summary.getUpdateTime(), dao.get(workflow.getWorkflowId(), "updateTime"));
        assertEquals(summary.getEndTime(), dao.get(workflow.getWorkflowId(), "endTime"));
        assertEquals(summary.getStatus().name(), dao.get(workflow.getWorkflowId(), "status"));
        assertEquals(summary.getInput(), dao.get(workflow.getWorkflowId(), "input"));
        assertEquals(summary.getOutput(), dao.get(workflow.getWorkflowId(), "output"));
        assertEquals(summary.getReasonForIncompletion(), dao.get(workflow.getWorkflowId(), "reasonForIncompletion"));
        assertEquals(String.valueOf(summary.getExecutionTime()), dao.get(workflow.getWorkflowId(), "executionTime"));
        assertEquals(summary.getEvent(), dao.get(workflow.getWorkflowId(), "event"));
        assertEquals(summary.getFailedReferenceTaskNames(), dao.get(workflow.getWorkflowId(), "failedReferenceTaskNames"));
    }

    private <T> List<T> tryFindResults(Supplier<List<T>> searchFunction) {
        return tryFindResults(searchFunction, 1);
    }

    private <T> List<T> tryFindResults(Supplier<List<T>> searchFunction, int resultsCount) {
        for (int i = 0; i < 20; i++) {
            List<T> result = searchFunction.get();
            if (result.size() >= resultsCount) {
                return result;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return Collections.emptyList();
    }

    private List<String> getTasks(Workflow workflow) {
        return dao.searchTasks("", "workflowId:\"" + workflow.getWorkflowId() + "\"", 0, 100, Collections.emptyList()).getResults();
    }

    private TaskExecLog createLog(String taskId, String log) {
        TaskExecLog taskExecLog = new TaskExecLog(log);
        taskExecLog.setTaskId(taskId);
        return taskExecLog;
    }

    private EventExecution createEventExecution(String event) {
        EventExecution execution = new EventExecution(uuid(), uuid());
        execution.setName("name");
        execution.setEvent(event);
        execution.setCreated(System.currentTimeMillis());
        execution.setStatus(EventExecution.Status.COMPLETED);
        execution.setAction(EventHandler.Action.Type.start_workflow);
        execution.setOutput(ImmutableMap.of("a", 1, "b", 2, "c", 3));
        return execution;
    }

    private String uuid() {
        return UUID.randomUUID().toString();
    }

}

