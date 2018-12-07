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

        assertWorkflowSummary(workflow.getWorkflowId(), summary);
    }

    @Test
    public void shouldIndexWorkflowAsync() throws Exception {
        Workflow workflow = TestUtils.loadWorkflowSnapshot("workflow");
        WorkflowSummary summary = new WorkflowSummary(workflow);

        dao.asyncIndexWorkflow(workflow).get();

        assertWorkflowSummary(workflow.getWorkflowId(), summary);
    }

    @Test
    public void shouldRemoveWorkflow() {
        Workflow workflow = TestUtils.loadWorkflowSnapshot("workflow");
        dao.indexWorkflow(workflow);

        // wait for workflow to be indexed
        List<String> workflows = tryFindResults(() -> searchWorkflows(workflow.getWorkflowId()), 1);
        assertEquals(1, workflows.size());

        dao.removeWorkflow(workflow.getWorkflowId());

        workflows = tryFindResults(() -> searchWorkflows(workflow.getWorkflowId()), 0);

        assertTrue("Workflow was not removed.", workflows.isEmpty());
    }

    @Test
    public void shouldAsyncRemoveWorkflow() throws Exception {
        Workflow workflow = TestUtils.loadWorkflowSnapshot("workflow");
        dao.indexWorkflow(workflow);

        // wait for workflow to be indexed
        List<String> workflows = tryFindResults(() -> searchWorkflows(workflow.getWorkflowId()), 1);
        assertEquals(1, workflows.size());

        dao.asyncRemoveWorkflow(workflow.getWorkflowId()).get();

        workflows = tryFindResults(() -> searchWorkflows(workflow.getWorkflowId()), 0);

        assertTrue("Workflow was not removed.", workflows.isEmpty());
    }

    @Test
    public void shouldUpdateWorkflow() {
        Workflow workflow = TestUtils.loadWorkflowSnapshot("workflow");
        WorkflowSummary summary = new WorkflowSummary(workflow);

        dao.indexWorkflow(workflow);

        dao.updateWorkflow(workflow.getWorkflowId(), new String[]{"status"}, new Object[]{Workflow.WorkflowStatus.COMPLETED});

        summary.setStatus(Workflow.WorkflowStatus.COMPLETED);
        assertWorkflowSummary(workflow.getWorkflowId(), summary);
    }

    @Test
    public void shouldAsyncUpdateWorkflow() throws Exception {
        Workflow workflow = TestUtils.loadWorkflowSnapshot("workflow");
        WorkflowSummary summary = new WorkflowSummary(workflow);

        dao.indexWorkflow(workflow);

        dao.asyncUpdateWorkflow(workflow.getWorkflowId(), new String[]{"status"}, new Object[]{Workflow.WorkflowStatus.FAILED}).get();

        summary.setStatus(Workflow.WorkflowStatus.FAILED);
        assertWorkflowSummary(workflow.getWorkflowId(), summary);
    }

    @Test
    public void shouldIndexTask() {
        Workflow workflow = TestUtils.loadWorkflowSnapshot("workflow");
        Task task = workflow.getTasks().get(0);

        TaskSummary summary = new TaskSummary(task);

        dao.indexTask(task);

        List<String> tasks = tryFindResults(() -> searchTasks(workflow));

        assertEquals(summary.getTaskId(), tasks.get(0));
    }

    @Test
    public void shouldIndexTaskAsync() throws Exception {
        Workflow workflow = TestUtils.loadWorkflowSnapshot("workflow");
        Task task = workflow.getTasks().get(0);

        TaskSummary summary = new TaskSummary(task);

        dao.asyncIndexTask(task).get();

        List<String> tasks = tryFindResults(() -> searchTasks(workflow));

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

    private void assertWorkflowSummary(String workflowId, WorkflowSummary summary) {
        assertEquals(summary.getWorkflowType(), dao.get(workflowId, "workflowType"));
        assertEquals(String.valueOf(summary.getVersion()), dao.get(workflowId, "version"));
        assertEquals(summary.getWorkflowId(), dao.get(workflowId, "workflowId"));
        assertEquals(summary.getCorrelationId(), dao.get(workflowId, "correlationId"));
        assertEquals(summary.getStartTime(), dao.get(workflowId, "startTime"));
        assertEquals(summary.getUpdateTime(), dao.get(workflowId, "updateTime"));
        assertEquals(summary.getEndTime(), dao.get(workflowId, "endTime"));
        assertEquals(summary.getStatus().name(), dao.get(workflowId, "status"));
        assertEquals(summary.getInput(), dao.get(workflowId, "input"));
        assertEquals(summary.getOutput(), dao.get(workflowId, "output"));
        assertEquals(summary.getReasonForIncompletion(), dao.get(workflowId, "reasonForIncompletion"));
        assertEquals(String.valueOf(summary.getExecutionTime()), dao.get(workflowId, "executionTime"));
        assertEquals(summary.getEvent(), dao.get(workflowId, "event"));
        assertEquals(summary.getFailedReferenceTaskNames(), dao.get(workflowId, "failedReferenceTaskNames"));
    }

    private <T> List<T> tryFindResults(Supplier<List<T>> searchFunction) {
        return tryFindResults(searchFunction, 1);
    }

    private <T> List<T> tryFindResults(Supplier<List<T>> searchFunction, int resultsCount) {
        List<T> result = Collections.emptyList();
        for (int i = 0; i < 20; i++) {
            result = searchFunction.get();
            if (result.size() == resultsCount) {
                return result;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return result;
    }

    private List<String> searchWorkflows(String workflowId) {
        return dao.searchWorkflows("", "workflowId:\"" + workflowId + "\"", 0, 100, Collections.emptyList()).getResults();
    }

    private List<String> searchTasks(Workflow workflow) {
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

