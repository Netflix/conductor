package com.netflix.conductor.core.execution.mapper;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.TerminateWorkflowException;
import com.netflix.conductor.dao.MetadataDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ConfluentKafkaPublishTaskMapper implements TaskMapper  {
	public static final Logger logger = LoggerFactory.getLogger(ConfluentKafkaPublishTaskMapper.class);

	private final ParametersUtils parametersUtils;
	private final MetadataDAO metadataDAO;

	public ConfluentKafkaPublishTaskMapper(ParametersUtils parametersUtils, MetadataDAO metadataDAO) {
		this.parametersUtils = parametersUtils;
		this.metadataDAO = metadataDAO;
	}

	/**
	 * This method maps a {@link WorkflowTask} of type {@link TaskType#KAFKA_PUBLISH}
	 * to a {@link Task} in a {@link Task.Status#SCHEDULED} state
	 *
	 * @param taskMapperContext: A wrapper class containing the {@link WorkflowTask}, {@link WorkflowDef}, {@link Workflow} and a string representation of the TaskId
	 * @return a List with just one Kafka task
	 * @throws TerminateWorkflowException In case if the task definition does not exist
	 */
	@Override
	public List<Task> getMappedTasks(TaskMapperContext taskMapperContext) throws TerminateWorkflowException {

		logger.debug("TaskMapperContext {} in ConfluentKafkaPublishTaskMapper", taskMapperContext);

		WorkflowTask taskToSchedule = taskMapperContext.getTaskToSchedule();
		Workflow workflowInstance = taskMapperContext.getWorkflowInstance();
		String taskId = taskMapperContext.getTaskId();
		int retryCount = taskMapperContext.getRetryCount();

		TaskDef taskDefinition = Optional.ofNullable(taskMapperContext.getTaskDefinition())
						.orElseGet(() -> Optional.ofNullable(metadataDAO.getTaskDef(taskToSchedule.getName()))
						.orElse(null));

		Map<String, Object> input = parametersUtils.getTaskInputV2(taskToSchedule.getInputParameters(), workflowInstance, taskId, taskDefinition);

		Task confluentKafkaPublishTask = new Task();
		confluentKafkaPublishTask.setTaskType(taskToSchedule.getType());
		confluentKafkaPublishTask.setTaskDefName(taskToSchedule.getName());
		confluentKafkaPublishTask.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
		confluentKafkaPublishTask.setWorkflowInstanceId(workflowInstance.getWorkflowId());
		confluentKafkaPublishTask.setWorkflowType(workflowInstance.getWorkflowName());
		confluentKafkaPublishTask.setCorrelationId(workflowInstance.getCorrelationId());
		confluentKafkaPublishTask.setScheduledTime(System.currentTimeMillis());
		confluentKafkaPublishTask.setTaskId(taskId);
		confluentKafkaPublishTask.setInputData(input);
		confluentKafkaPublishTask.setStatus(Task.Status.SCHEDULED);
		confluentKafkaPublishTask.setRetryCount(retryCount);
		confluentKafkaPublishTask.setCallbackAfterSeconds(taskToSchedule.getStartDelay());
		confluentKafkaPublishTask.setWorkflowTask(taskToSchedule);
		confluentKafkaPublishTask.setWorkflowPriority(workflowInstance.getPriority());
		if (Objects.nonNull(taskDefinition)) {
			confluentKafkaPublishTask.setExecutionNameSpace(taskDefinition.getExecutionNameSpace());
			confluentKafkaPublishTask.setIsolationGroupId(taskDefinition.getIsolationGroupId());
			confluentKafkaPublishTask.setRateLimitPerFrequency(taskDefinition.getRateLimitPerFrequency());
			confluentKafkaPublishTask.setRateLimitFrequencyInSeconds(taskDefinition.getRateLimitFrequencyInSeconds());
		}
		return Collections.singletonList(confluentKafkaPublishTask);
	}
}
