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
import java.util.Collections;
import java.util.List;

@Singleton
public class SimpleStatusHandler implements JavaEventAction {
    private static Logger logger = LoggerFactory.getLogger(SimpleStatusHandler.class);
    private static final String JQ_GET_WFID_URN = ".urns[] | select(startswith(\"urn:deluxe:conductor:workflow:\")) | split(\":\") [4]";
    private final WorkflowExecutor executor;
    private final ObjectMapper mapper;

    @Inject
    public SimpleStatusHandler(WorkflowExecutor executor, ObjectMapper mapper) {
        this.executor = executor;
        this.mapper = mapper;
    }

    @Override
    public List<String> handle(EventHandler.Action action, Object payload, EventExecution ee) throws Exception {
        ActionParams params = mapper.convertValue(action.getJava_action().getInputParameters(), ActionParams.class);
        if (StringUtils.isEmpty(params.taskRefName)) {
            throw new IllegalStateException("No taskRefName defined in parameters");
        }

        String workflowJq = StringUtils.defaultIfEmpty(params.workflowIdJq, JQ_GET_WFID_URN);
        String statusName = ScriptEvaluator.evalJq(".status.name", payload);
        String statusReason = ScriptEvaluator.evalJq(".status.reason", payload);
        String workflowId = ScriptEvaluator.evalJq(workflowJq, payload);
        if (StringUtils.isEmpty(workflowId)) {
            logger.debug("Skipping. No workflowId provided in urns");
            return Collections.emptyList();
        }

        Workflow workflow = executor.getWorkflow(workflowId, false);
        if (workflow == null) {
            logger.debug("Skipping. No workflow found for given id " + workflowId);
            return Collections.emptyList();
        }

        if (workflow.getStatus().isTerminal()) {
            logger.debug("Skipping. Target workflow is already " + workflow.getStatus().name()
                    + ", workflowId=" + workflow.getWorkflowId()
                    + ", contextUser=" + workflow.getContextUser()
                    + ", correlationId=" + workflow.getCorrelationId()
                    + ", traceId=" + workflow.getTraceId());
            return Collections.emptyList();
        }

        Task task = executor.getTask(workflowId, params.taskRefName);
        if (task == null) {
            logger.debug("Skipping. No task " + params.taskRefName + " found in workflow"
                    + ", workflowId=" + workflow.getWorkflowId()
                    + ", contextUser=" + workflow.getContextUser()
                    + ", correlationId=" + workflow.getCorrelationId()
                    + ", traceId=" + workflow.getTraceId());
            return Collections.emptyList();
        }

        if (task.getStatus().isTerminal()) {
            logger.debug("Skipping. Target task " + task + " is already finished. "
                    + ", workflowId=" + workflow.getWorkflowId()
                    + ", contextUser=" + workflow.getContextUser()
                    + ", correlationId=" + workflow.getCorrelationId()
                    + ", traceId=" + workflow.getTraceId());
            return Collections.emptyList();
        }
        TaskResult taskResult = new TaskResult(task);

        if (statusName.equalsIgnoreCase("Complete")) {
            task.setStatus(Task.Status.COMPLETED);
        } else if (statusName.equalsIgnoreCase("Failed")) {
            task.setStatus(Task.Status.FAILED);
            taskResult.setReasonForIncompletion(statusReason);
        } else if (statusName.equalsIgnoreCase("Cancelled")) {
            task.setStatus(Task.Status.CANCELED);
        } else if (statusName.equalsIgnoreCase("In_Queue")) {
            task.setStatus(Task.Status.IN_PROGRESS);
            taskResult.setResetStartTime(true);
        } else if (statusName.equalsIgnoreCase("Active")) {
            task.setStatus(Task.Status.IN_PROGRESS);
            taskResult.setResetStartTime(true);
        }


        if (params.payloadToOutput) {
            taskResult.setUpdateOutput(true);
            taskResult.getOutputData().put("payload", payload);
        }

        executor.updateTask(taskResult);
        logger.debug("Task " + task + " has been updated"
                + ", workflowId=" + workflow.getWorkflowId()
                + ", contextUser=" + workflow.getContextUser()
                + ", correlationId=" + workflow.getCorrelationId()
                + ", traceId=" + workflow.getTraceId());

        return Collections.singletonList(workflowId);
    }

    // Keep fields public!
    public static class ActionParams {
        public String taskRefName;
        public String workflowIdJq;
        public boolean payloadToOutput = false;
    }
}
