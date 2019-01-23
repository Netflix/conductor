package com.netflix.conductor.service;

import com.netflix.conductor.common.metadata.tasks.CompleteTaskResult;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author aniket.sinha on 1/19/19
 */
@Singleton
public class FloService {

    private enum FloStatus {
        DELIVERY_ASSIGNED,
        DELIVERY_CONFIRMED,
        DELIVERY_ARRIVED,
        DELIVERY_PICKED_UP,
        DELIVERY_REACHED,
        DELIVERY_DELIVERED
    }

    private static final String TASK_STATE_DELIMITER = ":__:";
    private static final String KEY_UNBLOCKED_BY = "unblocked_by";

    private final ExecutionService executionService;

    @Inject
    public FloService(ExecutionService executionService) {
        this.executionService = executionService;
    }

    public CompleteTaskResult completeWaitStateTask(String workflowId, String instanceState, String unblockedBy) {
        List<Task> runningTaskList = executionService.getInProgressWaitTasksForWorkflow(workflowId);

        if (runningTaskList.size() > 0) {
            Optional<Task> runningTaskOptional = getTaskOfInterest(runningTaskList, instanceState); // runningTaskList.get();
            if (runningTaskOptional.isPresent()) {
                Task runningTask = runningTaskOptional.get();
                String taskName = runningTask.getTaskDefName();
                if (isKnownState(taskName)) {
                    Map<String, Object> outputData = new HashMap<>();
                    outputData.put(KEY_UNBLOCKED_BY, unblockedBy);
                    // set outputData and status
                    runningTask.setOutputData(outputData);
                    runningTask.setStatus(Task.Status.COMPLETED);
                    executionService.updateTask(new TaskResult(runningTask));
                    return new CompleteTaskResult(CompleteTaskResult.CompletionStatus.COMPLETED, runningTask);
                } else {
                    return new CompleteTaskResult(CompleteTaskResult.CompletionStatus.UNKNOWN_STATE, runningTask);
                }
            }
            else {
                return new CompleteTaskResult(CompleteTaskResult.CompletionStatus.NO_IN_PROGRESS_WAIT_TASK_FOUND_FOR_STATE, null);
            }
        }
        return new CompleteTaskResult(CompleteTaskResult.CompletionStatus.NO_IN_PROGRESS_WAIT_TASK_FOUND_IN_WF, null);
    }

    private Optional<Task> getTaskOfInterest(List<Task> runningTaskList, String instanceState) {
        return runningTaskList
                .stream()
                .filter(task -> getStateFromTaskName(task.getTaskDefName()).equals(instanceState))
                .findFirst();
    }


    private String getStateFromTaskName(String taskName) {
        return taskName.split(TASK_STATE_DELIMITER)[0].toUpperCase();
    }

    private boolean isKnownState(String taskName) {
        String taskInstanceState = getStateFromTaskName(taskName);
        String concatStatus = FloStatus.DELIVERY_ASSIGNED.name() + "|" +
                FloStatus.DELIVERY_CONFIRMED.name() + "|" +
                FloStatus.DELIVERY_ARRIVED.name() + "|" +
                FloStatus.DELIVERY_PICKED_UP.name() + "|" +
                FloStatus.DELIVERY_REACHED.name() + "|" +
                FloStatus.DELIVERY_DELIVERED.name();

        return concatStatus.indexOf(taskInstanceState) != -1;
    }
}
