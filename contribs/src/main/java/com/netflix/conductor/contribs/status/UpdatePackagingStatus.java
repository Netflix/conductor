package com.netflix.conductor.contribs.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.JavaEventAction;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.function.Consumer;

@Singleton
public class UpdatePackagingStatus implements JavaEventAction {
    private static Logger logger = LoggerFactory.getLogger(UpdatePackagingStatus.class);
    private static final String JQ_GET_WFID_URN = ".urns[] | select(startswith(\"urn:deluxe:conductor:workflow:\")) | split(\":\") [4]";

    private final WorkflowExecutor executor;
    private final ObjectMapper mapper;

    @Inject
    public UpdatePackagingStatus(WorkflowExecutor executor, ObjectMapper mapper) {
        this.executor = executor;
        this.mapper = mapper;
    }

    public List<?> handle(EventHandler.Action action, Object payload, EventExecution ee) throws Exception {
        ActionParams params = mapper.convertValue(action.getJava_action().getInputParameters(), ActionParams.class);

        String workflowId = StringUtils.defaultIfEmpty(params.workflowIdJq, JQ_GET_WFID_URN);
        String statusName = ScriptEvaluator.evalJq(".Status.Name", payload);
        String statusReason = ScriptEvaluator.evalJq(".Status.Reason", payload);

        if (StringUtils.isEmpty(workflowId)) {
            logger.debug("Skipping. No workflowId provided in urns");
            return Collections.singletonList(UUID.randomUUID().toString());//Random UUId is returned to handle the retryEnbaled=true case.This will prevent retrying
        }

        Workflow workflow = executor.getWorkflow(workflowId, false);
        if (workflow == null) {
            logger.debug("Skipping. No workflow found for given id " + workflowId);
            return Collections.singletonList(UUID.randomUUID().toString());
        }

        if (workflow.getStatus().isTerminal()) {
            logger.debug("Skipping. Target workflow is already " + workflow.getStatus().name()
                    + ", workflowId=" + workflow.getWorkflowId()
                    + ", contextUser=" + workflow.getContextUser()
                    + ", correlationId=" + workflow.getCorrelationId()
                    + ", traceId=" + workflow.getTraceId());
            return Collections.singletonList(UUID.randomUUID().toString());
        }

        Task task = executor.getTask(workflowId, params.taskRefName);
        if (task == null) {
            logger.debug("Skipping. No task " + params.taskRefName + " found in workflow"
                    + ", workflowId=" + workflow.getWorkflowId()
                    + ", contextUser=" + workflow.getContextUser()
                    + ", correlationId=" + workflow.getCorrelationId()
                    + ", traceId=" + workflow.getTraceId());
            return Collections.singletonList(UUID.randomUUID().toString());
        }

        if (task.getStatus().isTerminal()) {
            logger.debug("Skipping. Target task " + task + " is already finished. "
                    + ", workflowId=" + workflow.getWorkflowId()
                    + ", contextUser=" + workflow.getContextUser()
                    + ", correlationId=" + workflow.getCorrelationId()
                    + ", traceId=" + workflow.getTraceId());
            return Collections.singletonList(UUID.randomUUID().toString());
        }

        TaskResult taskResult = new TaskResult(task);

        if ("Complete".equalsIgnoreCase(statusName)) {
            taskResult.setStatus(TaskResult.Status.COMPLETED);
        } else if ("Failed".equalsIgnoreCase(statusName)) {
            taskResult.setStatus(TaskResult.Status.FAILED);
            taskResult.setReasonForIncompletion(statusReason);
        } else if ("Cancelled".equalsIgnoreCase(statusName)) {
            taskResult.setStatus(TaskResult.Status.CANCELED);
        } else if ("pending".equalsIgnoreCase(statusName)) {
            taskResult.setStatus(TaskResult.Status.IN_PROGRESS);
            taskResult.setResetStartTime(true);
        } else if ("in-progress".equalsIgnoreCase(statusName)) {
            taskResult.setStatus(TaskResult.Status.IN_PROGRESS);
            taskResult.setResetStartTime(true);
        }else{
            logger.debug("Handler for status value {} is not registered", statusName);
            return Collections.singletonList(UUID.randomUUID().toString());
        }

        if (params.payloadToOutput) {
            taskResult.setUpdateOutput(true);
            if (taskResult.getOutputData() != null) {
                taskResult.getOutputData().put("payload", payload);
            }
        }

        executor.updateTask(taskResult);
        logger.debug("Task " + task + " has been updated"
                + ", workflowId=" + workflow.getWorkflowId()
                + ", contextUser=" + workflow.getContextUser()
                + ", correlationId=" + workflow.getCorrelationId()
                + ", traceId=" + workflow.getTraceId());

        return Collections.singletonList(workflowId);
    }

    private String getTaskStatus(Task targetTask) {
        return Optional.of(targetTask.getStatus()).orElseThrow(
                () -> new IllegalArgumentException("No status in the task " + targetTask.getTaskId())).name();
    }

    public static class ActionParams {
        public String taskRefName;
        public String workflowIdJq;
        public boolean payloadToOutput = false;
    }
}
