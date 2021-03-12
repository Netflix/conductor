/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.test.integration

import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.tasks.TaskType
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.run.Workflow
import com.netflix.conductor.core.execution.WorkflowRepairService
import com.netflix.conductor.core.execution.WorkflowSweeper
import com.netflix.conductor.core.execution.tasks.SubWorkflow
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask
import com.netflix.conductor.dao.QueueDAO
import com.netflix.conductor.test.base.AbstractSpecification
import org.springframework.beans.factory.annotation.Autowired
import spock.lang.Shared

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_SUB_WORKFLOW
import static com.netflix.conductor.test.util.WorkflowTestUtil.verifyPolledAndAcknowledgedTask

class SubWorkflowSpec extends AbstractSpecification {

    @Autowired
    QueueDAO queueDAO

    @Autowired
    WorkflowSweeper workflowSweeper

    @Autowired
    WorkflowRepairService workflowRepairService

    @Shared
    def WORKFLOW_WITH_SUBWORKFLOW = 'integration_test_wf_with_sub_wf'

    @Shared
    def SUB_WORKFLOW = "sub_workflow"

    @Shared
    def SIMPLE_WORKFLOW = "integration_test_wf"

    def setup() {
        workflowTestUtil.registerWorkflows('simple_one_task_sub_workflow_integration_test.json',
                'simple_workflow_1_integration_test.json',
                'workflow_with_sub_workflow_1_integration_test.json')
    }

    def "Test retrying a subworkflow where parent workflow timed out due to workflowTimeout"() {

        setup: "Register a workflow definition with a timeout policy set to timeout workflow"
        def persistedWorkflowDefinition = metadataService.getWorkflowDef(WORKFLOW_WITH_SUBWORKFLOW, 1)
        def modifiedWorkflowDefinition = new WorkflowDef()
        modifiedWorkflowDefinition.name = persistedWorkflowDefinition.name
        modifiedWorkflowDefinition.version = persistedWorkflowDefinition.version
        modifiedWorkflowDefinition.tasks = persistedWorkflowDefinition.tasks
        modifiedWorkflowDefinition.inputParameters = persistedWorkflowDefinition.inputParameters
        modifiedWorkflowDefinition.outputParameters = persistedWorkflowDefinition.outputParameters
        modifiedWorkflowDefinition.timeoutPolicy = WorkflowDef.TimeoutPolicy.TIME_OUT_WF
        modifiedWorkflowDefinition.timeoutSeconds = 10
        modifiedWorkflowDefinition.ownerEmail = persistedWorkflowDefinition.ownerEmail
        metadataService.updateWorkflowDef([modifiedWorkflowDefinition])

        and: "an existing workflow with subworkflow and registered definitions"
        metadataService.getWorkflowDef(SUB_WORKFLOW, 1)
        metadataService.getWorkflowDef(WORKFLOW_WITH_SUBWORKFLOW, 1)

        and: "input required to start the workflow execution"
        String correlationId = 'wf_with_subwf_test_1'
        def input = new HashMap()
        String inputParam1 = 'p1 value'
        input['param1'] = inputParam1
        input['param2'] = 'p2 value'
        input['subwf'] = 'sub_workflow'

        when: "the workflow is started"
        def workflowInstanceId = workflowExecutor.startWorkflow(WORKFLOW_WITH_SUBWORKFLOW, 1,
                correlationId, input, null, null, null)

        then: "verify that the workflow is in a RUNNING state"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and complete the integration_task_1 task"
        def pollAndCompleteTask = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the 'integration_task_1' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask)

        and: "verify that the 'integration_task1' is complete and the next task (subworkflow) is in SCHEDULED state"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'SUB_WORKFLOW'
            tasks[1].status == Task.Status.SCHEDULED
        }

        when: "the subworkflow is started by issuing a system task call"
        List<String> polledTaskIds = queueDAO.pop("SUB_WORKFLOW", 1, 200)
        String subworkflowTaskId = polledTaskIds.get(0)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), subworkflowTaskId, 30)

        then: "verify that the 'sub_workflow_task' is in a IN_PROGRESS state"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TaskType.SUB_WORKFLOW.name()
            tasks[1].status == Task.Status.IN_PROGRESS
        }

        when: "subworkflow is retrieved"
        def workflow = workflowExecutionService.getExecutionStatus(workflowInstanceId, true)
        def subWorkflowId = workflow.tasks[1].subWorkflowId

        then: "verify that the sub workflow is RUNNING, and first task is in SCHEDULED state"
        with(workflowExecutionService.getExecutionStatus(subWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'simple_task_in_sub_wf'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "a delay of 10 seconds is introduced and the workflow is sweeped to run the evaluation"
        Thread.sleep(10000)
        workflowSweeper.sweep([workflowInstanceId], workflowExecutor, workflowRepairService)

        then: "ensure that the workflow has been TIMED OUT and subworkflow task is CANCELED"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.TIMED_OUT
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TaskType.SUB_WORKFLOW.name()
            tasks[1].status == Task.Status.CANCELED
        }

        and: "ensure that the subworkflow is TERMINATED and task is CANCELED"
        with(workflowExecutionService.getExecutionStatus(subWorkflowId, true)) {
            status == Workflow.WorkflowStatus.TERMINATED
            tasks.size() == 1
            tasks[0].taskType == 'simple_task_in_sub_wf'
            tasks[0].status == Task.Status.CANCELED
        }

        when: "the subworkflow is retried"
        workflowExecutor.retry(subWorkflowId, false)

        then: "ensure that the subworkflow is RUNNING and task is retried"
        with(workflowExecutionService.getExecutionStatus(subWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'simple_task_in_sub_wf'
            tasks[0].status == Task.Status.CANCELED
            tasks[1].taskType == 'simple_task_in_sub_wf'
            tasks[1].status == Task.Status.SCHEDULED
        }

        and: "verify that change flag is set on the sub workflow task in parent"
        workflowExecutionService.getTask(subworkflowTaskId).subworkflowChanged

        when: "Polled for simple_task_in_sub_wf task in subworkflow"
        pollAndCompleteTask = workflowTestUtil.pollAndCompleteTask('simple_task_in_sub_wf', 'task1.integration.worker', ['op': 'simple_task_in_sub_wf.done'])

        then: "verify that the 'simple_task_in_sub_wf' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask)

        and: "verify that the subworkflow is in a completed state"
        with(workflowExecutionService.getExecutionStatus(subWorkflowId, true)) {
            status == Workflow.WorkflowStatus.COMPLETED
            tasks.size() == 2
            tasks[0].taskType == 'simple_task_in_sub_wf'
            tasks[0].status == Task.Status.CANCELED
            tasks[1].taskType == 'simple_task_in_sub_wf'
            tasks[1].status == Task.Status.COMPLETED
        }

        and: "subworkflow task is in a completed state"
        with(workflowExecutionService.getTask(subworkflowTaskId)) {
            status == Task.Status.COMPLETED
            subworkflowChanged
        }

        and: "the parent workflow is swept"
        workflowSweeper.sweep([workflowInstanceId], workflowExecutor, workflowRepairService)

        and: "the parent workflow has been resumed"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.COMPLETED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TaskType.SUB_WORKFLOW.name()
            tasks[1].status == Task.Status.COMPLETED
            output['op'] == 'simple_task_in_sub_wf.done'
        }

        cleanup: "Ensure that the changes to the workflow def are reverted"
        metadataService.updateWorkflowDef([persistedWorkflowDefinition])
    }

    def "Test terminating a subworkflow terminates parent workflow"() {
        given: "Existing workflow and subworkflow definitions"
        metadataService.getWorkflowDef(SUB_WORKFLOW, 1)
        metadataService.getWorkflowDef(WORKFLOW_WITH_SUBWORKFLOW, 1)

        and: "input required to start the workflow execution"
        String correlationId = 'wf_with_subwf_test_1'
        def input = new HashMap()
        String inputParam1 = 'p1 value'
        input['param1'] = inputParam1
        input['param2'] = 'p2 value'
        input['subwf'] = 'sub_workflow'

        when: "Start a workflow with subworkflow based on the registered definition"
        def workflowInstanceId = workflowExecutor.startWorkflow(WORKFLOW_WITH_SUBWORKFLOW, 1,
                correlationId, input,
                null, null, null)

        then: "verify that the workflow is in a running state"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "Polled for integration_task_1 task"
        def pollAndCompleteTask1Try1 = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the 'integration_task_1' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask1Try1)

        and: "verify that the 'integration_task1' is complete and the next task (subworkflow) is in scheduled state"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'SUB_WORKFLOW'
            tasks[1].status == Task.Status.SCHEDULED
        }

        when: "Polled for and executed subworkflow task"
        List<String> polledTaskIds = queueDAO.pop("SUB_WORKFLOW", 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)
        def workflow = workflowExecutionService.getExecutionStatus(workflowInstanceId, true)
        def subWorkflowId = workflow.tasks[1].subWorkflowId

        then: "verify that the 'sub_workflow_task' is polled and IN_PROGRESS"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'SUB_WORKFLOW'
            tasks[1].status == Task.Status.IN_PROGRESS
        }

        and: "verify that the sub workflow is RUNNING, and first task is in SCHEDULED state"
        with(workflowExecutionService.getExecutionStatus(subWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'simple_task_in_sub_wf'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "subworkflow is terminated"
        def terminateReason = "terminating from a test case"
        workflowExecutor.terminateWorkflow(subWorkflowId, terminateReason)

        then: "verify that sub workflow is in terminated state"
        with(workflowExecutionService.getExecutionStatus(subWorkflowId, true)) {
            status == Workflow.WorkflowStatus.TERMINATED
            tasks.size() == 1
            tasks[0].taskType == 'simple_task_in_sub_wf'
            tasks[0].status == Task.Status.CANCELED
            reasonForIncompletion == terminateReason
        }

        and:
        workflowSweeper.sweep([workflowInstanceId], workflowExecutor, workflowRepairService)

        and: "verify that parent workflow is in terminated state"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.TERMINATED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'SUB_WORKFLOW'
            tasks[1].status == Task.Status.CANCELED
            reasonForIncompletion == terminateReason
        }
    }

    def "Test retrying a workflow with subworkflow resume"() {
        setup: "Modify task definition to 0 retries"
        def persistedTask2Definition = workflowTestUtil.getPersistedTaskDefinition('integration_task_2').get()
        def modifiedTask2Definition = new TaskDef(persistedTask2Definition.name, persistedTask2Definition.description,
                persistedTask2Definition.ownerEmail, 0, persistedTask2Definition.timeoutSeconds,
                persistedTask2Definition.responseTimeoutSeconds)
        metadataService.updateTaskDef(modifiedTask2Definition)

        and: "an existing workflow with subworkflow and registered definitions"
        metadataService.getWorkflowDef(SIMPLE_WORKFLOW, 1)
        metadataService.getWorkflowDef(WORKFLOW_WITH_SUBWORKFLOW, 1)

        and: "input required to start the workflow execution"
        String correlationId = 'wf_retry_with_subwf_resume_test'
        def input = new HashMap()
        String inputParam1 = 'p1 value'
        input['param1'] = inputParam1
        input['param2'] = 'p2 value'
        input['subwf'] = 'integration_test_wf'

        when: "the workflow is started"
        def workflowInstanceId = workflowExecutor.startWorkflow(WORKFLOW_WITH_SUBWORKFLOW, 1,
                correlationId, input, null, null, null)

        then: "verify that the workflow is in a RUNNING state"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and complete the integration_task_1 task"
        def pollAndCompleteTask = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the 'integration_task_1' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask)

        and: "verify that the 'integration_task_1' is complete and the next task (subworkflow) is in SCHEDULED state"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'SUB_WORKFLOW'
            tasks[1].status == Task.Status.SCHEDULED
        }

        when: "the subworkflow is started by issuing a system task call"
        List<String> polledTaskIds = queueDAO.pop("SUB_WORKFLOW", 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)

        then: "verify that the 'sub_workflow_task' is in a IN_PROGRESS state"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TaskType.SUB_WORKFLOW.name()
            tasks[1].status == Task.Status.IN_PROGRESS
        }

        when: "subworkflow is retrieved"
        def workflow = workflowExecutionService.getExecutionStatus(workflowInstanceId, true)
        def subWorkflowId = workflow.tasks[1].subWorkflowId

        then: "verify that the sub workflow is RUNNING, and first task is in SCHEDULED state"
        with(workflowExecutionService.getExecutionStatus(subWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and complete the integration_task_1 task"
        pollAndCompleteTask = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the 'integration_task_1' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask)

        and: "verify that the 'integration_task_1' is complete and the next task is in SCHEDULED state"
        with(workflowExecutionService.getExecutionStatus(subWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.SCHEDULED
        }

        when: "poll and fail the integration_task_2 task"
        def pollAndFailTask = workflowTestUtil.pollAndFailTask('integration_task_2', 'task2.integration.worker', 'failed')

        then: "verify that the 'integration_task_2' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndFailTask)

        then: "the sub workflow ends up in a FAILED state"
        with(workflowExecutionService.getExecutionStatus(subWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
        }

        and:
        workflowSweeper.sweep([workflowInstanceId], workflowExecutor, workflowRepairService)

        and: "the workflow is in a FAILED state"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'SUB_WORKFLOW'
            tasks[1].status == Task.Status.FAILED
        }

        when: "the workflow is retried by resuming subworkflow task"
        workflowExecutor.retry(workflowInstanceId, true)

        then: "the subworkflow is in a RUNNING state"
        with(workflowExecutionService.getExecutionStatus(subWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 3
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
            tasks[2].taskType == 'integration_task_2'
            tasks[2].status == Task.Status.SCHEDULED
        }

        and: "the workflow is in a RUNNING state"
        workflowSweeper.sweep([workflowInstanceId], workflowExecutor, workflowRepairService)
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'SUB_WORKFLOW'
            tasks[1].status == Task.Status.IN_PROGRESS
        }

        when: "poll and complete the integration_task_2 task"
        pollAndCompleteTask = workflowTestUtil.pollAndCompleteTask('integration_task_2', 'task2.integration.worker', ['op': 'task2.done'])

        then: "verify that the 'integration_task_2' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask)

        then: "the integration_task_2 is complete sub workflow ends up in a COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(subWorkflowId, true)) {
            status == Workflow.WorkflowStatus.COMPLETED
            tasks.size() == 3
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
            tasks[2].taskType == 'integration_task_2'
            tasks[2].status == Task.Status.COMPLETED
        }

        and:
        workflowSweeper.sweep([workflowInstanceId], workflowExecutor, workflowRepairService)

        then: "the workflow is in a COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.COMPLETED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'SUB_WORKFLOW'
            tasks[1].status == Task.Status.COMPLETED
        }

        cleanup: "Ensure that changes to the task def are reverted"
        metadataService.updateTaskDef(persistedTask2Definition)
    }

    /**
     * On a 3-level workflow where all workflows reach FAILED state because of a FAILED task
     * in the leaf workflow.
     *
     * A retry is executed on the root workflow.
     *
     * Expectation: The root workflow spawns a NEW mid-level workflow, which in turn spawns a NEW leaf workflow.
     * When the leaf workflow completes successfully, both the NEW mid-level and root workflows also complete successfully.
     */
    def "Test retry on the root in a 3-level subworkflow"() {
        //region Test setup: 3 workflows reach FAILED state. Task 'integration_task_2' in leaf workflow is FAILED.
        setup: "Modify task definition to 0 retries"
        def persistedTask2Definition = workflowTestUtil.getPersistedTaskDefinition('integration_task_2').get()
        def modifiedTask2Definition = new TaskDef(persistedTask2Definition.name, persistedTask2Definition.description,
                persistedTask2Definition.ownerEmail, 0, persistedTask2Definition.timeoutSeconds,
                persistedTask2Definition.responseTimeoutSeconds)
        metadataService.updateTaskDef(modifiedTask2Definition)

        and: "an existing workflow with subworkflow and registered definitions"
        metadataService.getWorkflowDef(SIMPLE_WORKFLOW, 1)
        metadataService.getWorkflowDef(WORKFLOW_WITH_SUBWORKFLOW, 1)

        and: "input required to start the workflow execution"
        String correlationId = 'retry_on_root_in_3level_wf'
        def input = [
                'param1'   : 'p1 value',
                'param2'   : 'p2 value',
                'subwf'    : WORKFLOW_WITH_SUBWORKFLOW,
                'nextSubwf': SIMPLE_WORKFLOW]

        when: "the workflow is started"
        def rootWorkflowId = workflowExecutor.startWorkflow(WORKFLOW_WITH_SUBWORKFLOW, 1,
                correlationId, input, null, null, null)

        then: "verify that the workflow is in a RUNNING state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and complete the integration_task_1 task"
        def pollAndCompleteTask = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the 'integration_task_1' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask)

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        List<String> polledTaskIds = queueDAO.pop("SUB_WORKFLOW", 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)

        then: "verify that the 'sub_workflow_task' is in a IN_PROGRESS state"
        def rootWorkflowInstance = workflowExecutionService.getExecutionStatus(rootWorkflowId, true)
        with(rootWorkflowInstance) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
        }

        and: "verify that the mid-level workflow is RUNNING, and first task is in SCHEDULED state"
        def midLevelWorkflowId = rootWorkflowInstance.tasks[1].subWorkflowId
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        and: "poll and complete the integration_task_1 task in the mid-level workflow"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        polledTaskIds = queueDAO.pop(TASK_TYPE_SUB_WORKFLOW, 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)
        def midLevelWorkflowInstance = workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)

        then: "verify that the mid-level workflow is RUNNING, and first task is in SCHEDULED state"
        def leafWorkflowId = midLevelWorkflowInstance.tasks[1].subWorkflowId
        def leafWorkflowInstance = workflowExecutionService.getExecutionStatus(leafWorkflowId, true)
        with(leafWorkflowInstance) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and fail the integration_task_2 task"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])
        workflowTestUtil.pollAndFailTask('integration_task_2', 'task2.integration.worker', 'failed')

        then: "the leaf workflow ends up in a FAILED state"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
        }

        when: "the mid level workflow is 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)

        then: "the mid level subworkflow is in FAILED state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
        }

        when: "the root level workflow is 'decided'"
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "the root level workflow is in FAILED state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
        }
        //endregion

        //region Test case
        when: "do a retry on the root workflow"
        workflowExecutor.retry(rootWorkflowId, false)

        then: "poll and complete the 'integration_task_1' task"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op1': 'task1.done'])

        and: "verify that the root workflow created a new SUB_WORKFLOW task"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 3
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
            tasks[1].retried
            tasks[2].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[2].status == Task.Status.SCHEDULED
            tasks[2].retriedTaskId == tasks[1].taskId
        }

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        polledTaskIds = queueDAO.pop(TASK_TYPE_SUB_WORKFLOW, 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds[0], 30)
        def newMidLevelWorkflowId = workflowExecutionService.getTask(polledTaskIds[0]).subWorkflowId

        then: "verify that a new mid level workflow is created and is in RUNNING state"
        newMidLevelWorkflowId != midLevelWorkflowId
        with(workflowExecutionService.getExecutionStatus(newMidLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
        }

        when: "poll and complete the integration_task_1 task in the mid-level workflow"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        and: "poll and execute the sub workflow task"
        polledTaskIds = queueDAO.pop(TASK_TYPE_SUB_WORKFLOW, 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds[0], 30)
        def newLeafWorkflowId = workflowExecutionService.getTask(polledTaskIds[0]).subWorkflowId

        then: "verify that a new leaf workflow is created and is in RUNNING state"
        newLeafWorkflowId != leafWorkflowId
        with(workflowExecutionService.getExecutionStatus(newLeafWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
        }

        when: "poll and complete the two tasks in the leaf workflow"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])
        workflowTestUtil.pollAndCompleteTask('integration_task_2', 'task2.integration.worker', ['op': 'task2.done'])

        then: "the new leaf workflow is in COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(newLeafWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }

        when: "the new mid level and root workflows are 'decided'"
        workflowSweeper.sweep([newMidLevelWorkflowId], workflowExecutor, workflowRepairService)
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "the new mid level workflow is in COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(newMidLevelWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }

        then: "the root workflow is in COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }
        //endregion

        cleanup: "Ensure that changes to the task def are reverted"
        metadataService.updateTaskDef(persistedTask2Definition)
    }

    /**
     * On a 3-level workflow where all workflows reach FAILED state because of a FAILED task
     * in the leaf workflow.
     *
     * A retry is executed with resume flag on the root workflow.
     *
     * Expectation: The leaf workflow is retried and both its parent (mid-level) and grand parent (root) workflows are also retried.
     * When the leaf workflow completes successfully, both the mid-level and root workflows also complete successfully.
     */
    def "Test retry on the root with resume flag in a 3-level subworkflow"() {
        //region Test setup: 3 workflows reach FAILED state. Task 'integration_task_2' in leaf workflow is FAILED.
        setup: "Modify task definition to 0 retries"
        def persistedTask2Definition = workflowTestUtil.getPersistedTaskDefinition('integration_task_2').get()
        def modifiedTask2Definition = new TaskDef(persistedTask2Definition.name, persistedTask2Definition.description,
                persistedTask2Definition.ownerEmail, 0, persistedTask2Definition.timeoutSeconds,
                persistedTask2Definition.responseTimeoutSeconds)
        metadataService.updateTaskDef(modifiedTask2Definition)

        and: "an existing workflow with subworkflow and registered definitions"
        metadataService.getWorkflowDef(SIMPLE_WORKFLOW, 1)
        metadataService.getWorkflowDef(WORKFLOW_WITH_SUBWORKFLOW, 1)

        and: "input required to start the workflow execution"
        String correlationId = 'retry_on_root_in_3level_wf'
        def input = [
                'param1'   : 'p1 value',
                'param2'   : 'p2 value',
                'subwf'    : WORKFLOW_WITH_SUBWORKFLOW,
                'nextSubwf': SIMPLE_WORKFLOW]

        when: "the workflow is started"
        def rootWorkflowId = workflowExecutor.startWorkflow(WORKFLOW_WITH_SUBWORKFLOW, 1,
                correlationId, input, null, null, null)

        then: "verify that the workflow is in a RUNNING state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and complete the integration_task_1 task"
        def pollAndCompleteTask = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the 'integration_task_1' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask)

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        List<String> polledTaskIds = queueDAO.pop("SUB_WORKFLOW", 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)

        then: "verify that the 'sub_workflow_task' is in a IN_PROGRESS state"
        def rootWorkflowInstance = workflowExecutionService.getExecutionStatus(rootWorkflowId, true)
        with(rootWorkflowInstance) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
        }

        and: "verify that the mid-level workflow is RUNNING, and first task is in SCHEDULED state"
        def midLevelWorkflowId = rootWorkflowInstance.tasks[1].subWorkflowId
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        and: "poll and complete the integration_task_1 task in the mid-level workflow"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        polledTaskIds = queueDAO.pop(TASK_TYPE_SUB_WORKFLOW, 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)
        def midLevelWorkflowInstance = workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)

        then: "verify that the mid-level workflow is RUNNING, and first task is in SCHEDULED state"
        def leafWorkflowId = midLevelWorkflowInstance.tasks[1].subWorkflowId
        def leafWorkflowInstance = workflowExecutionService.getExecutionStatus(leafWorkflowId, true)
        with(leafWorkflowInstance) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and fail the integration_task_2 task"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])
        workflowTestUtil.pollAndFailTask('integration_task_2', 'task2.integration.worker', 'failed')

        then: "the leaf workflow ends up in a FAILED state"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
        }

        when: "the mid level workflow is 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)

        then: "the mid level subworkflow is in FAILED state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
        }

        when: "the root level workflow is 'decided'"
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "the root level workflow is in FAILED state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
        }
        //endregion

        //region Test case
        when: "do a retry on the root workflow"
        workflowExecutor.retry(rootWorkflowId, true)

        then: "verify that the sub workflow task in root workflow is IN_PROGRESS state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            tasks[1].subworkflowChanged
        }

        and: "verify that the sub workflow task in mid level workflow is IN_PROGRESS state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            tasks[1].subworkflowChanged
        }

        and: "verify that the previously failed task in leaf workflow is in SCHEDULED state"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 3
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
            tasks[1].retried
            tasks[2].taskType == 'integration_task_2'
            tasks[2].status == Task.Status.SCHEDULED
        }

        when: "the mid level and root workflows are 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "the mid level workflow is in RUNNING state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            !tasks[1].subworkflowChanged // flag is reset after "decide"
        }

        and: "the root workflow is in RUNNING state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            !tasks[1].subworkflowChanged // flag is reset after "decide"
        }

        when: "poll and complete the integration_task_2 task"
        workflowTestUtil.pollAndCompleteTask('integration_task_2', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the leaf workflow is in COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }

        when: "the mid level and root workflows are 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "the new mid level workflow is in COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }

        and: "the root workflow is in COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }
        //endregion

        cleanup: "Ensure that changes to the task def are reverted"
        metadataService.updateTaskDef(persistedTask2Definition)
    }

    /**
     * On a 3-level workflow where all workflows reach FAILED state because of a FAILED task
     * in the leaf workflow.
     *
     * A retry is executed on the mid-level workflow.
     *
     * Expectation: The mid-level workflow spawns a NEW leaf workflow and also updates its parent (root workflow).
     * When the NEW leaf workflow completes successfully, both the mid-level and root workflows also complete successfully.
     */
    def "Test retry on the mid-level in a 3-level subworkflow"() {
        //region Test setup: 3 workflows reach FAILED state. Task 'integration_task_2' in leaf workflow is FAILED.
        setup: "Modify task definition to 0 retries"
        def persistedTask2Definition = workflowTestUtil.getPersistedTaskDefinition('integration_task_2').get()
        def modifiedTask2Definition = new TaskDef(persistedTask2Definition.name, persistedTask2Definition.description,
                persistedTask2Definition.ownerEmail, 0, persistedTask2Definition.timeoutSeconds,
                persistedTask2Definition.responseTimeoutSeconds)
        metadataService.updateTaskDef(modifiedTask2Definition)

        and: "an existing workflow with subworkflow and registered definitions"
        metadataService.getWorkflowDef(SIMPLE_WORKFLOW, 1)
        metadataService.getWorkflowDef(WORKFLOW_WITH_SUBWORKFLOW, 1)

        and: "input required to start the workflow execution"
        String correlationId = 'retry_on_root_in_3level_wf'
        def input = [
                'param1'   : 'p1 value',
                'param2'   : 'p2 value',
                'subwf'    : WORKFLOW_WITH_SUBWORKFLOW,
                'nextSubwf': SIMPLE_WORKFLOW]

        when: "the workflow is started"
        def rootWorkflowId = workflowExecutor.startWorkflow(WORKFLOW_WITH_SUBWORKFLOW, 1,
                correlationId, input, null, null, null)

        then: "verify that the workflow is in a RUNNING state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and complete the integration_task_1 task"
        def pollAndCompleteTask = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the 'integration_task_1' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask)

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        List<String> polledTaskIds = queueDAO.pop("SUB_WORKFLOW", 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)

        then: "verify that the 'sub_workflow_task' is in a IN_PROGRESS state"
        def rootWorkflowInstance = workflowExecutionService.getExecutionStatus(rootWorkflowId, true)
        with(rootWorkflowInstance) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
        }

        and: "verify that the mid-level workflow is RUNNING, and first task is in SCHEDULED state"
        def midLevelWorkflowId = rootWorkflowInstance.tasks[1].subWorkflowId
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        and: "poll and complete the integration_task_1 task in the mid-level workflow"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        polledTaskIds = queueDAO.pop(TASK_TYPE_SUB_WORKFLOW, 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)
        def midLevelWorkflowInstance = workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)

        then: "verify that the mid-level workflow is RUNNING, and first task is in SCHEDULED state"
        def leafWorkflowId = midLevelWorkflowInstance.tasks[1].subWorkflowId
        def leafWorkflowInstance = workflowExecutionService.getExecutionStatus(leafWorkflowId, true)
        with(leafWorkflowInstance) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and fail the integration_task_2 task"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])
        workflowTestUtil.pollAndFailTask('integration_task_2', 'task2.integration.worker', 'failed')

        then: "the leaf workflow ends up in a FAILED state"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
        }

        when: "the mid level workflow is 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)

        then: "the mid level subworkflow is in FAILED state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
        }

        when: "the root level workflow is 'decided'"
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "the root level workflow is in FAILED state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
        }
        //endregion

        //region Test case
        when: "do a retry on the mid level workflow"
        workflowExecutor.retry(midLevelWorkflowId, false)

        then: "verify that the mid workflow created a new SUB_WORKFLOW task"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 3
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
            tasks[1].retried
            tasks[2].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[2].status == Task.Status.SCHEDULED
            tasks[2].retriedTaskId == tasks[1].taskId
        }

        and: "verify the SUB_WORKFLOW task in root workflow is IN_PROGRESS state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            tasks[1].subworkflowChanged
        }

        when: "the SUB_WORKFLOW task in mid level workflow is started by issuing a system task call"
        polledTaskIds = queueDAO.pop(TASK_TYPE_SUB_WORKFLOW, 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds[0], 30)
        def newLeafWorkflowId = workflowExecutionService.getTask(polledTaskIds[0]).subWorkflowId

        then: "verify that a new leaf workflow is created and is in RUNNING state"
        newLeafWorkflowId != leafWorkflowId
        with(workflowExecutionService.getExecutionStatus(newLeafWorkflowId, false)) {
            status == Workflow.WorkflowStatus.RUNNING
        }

        when: "poll and complete the 2 tasks in the leaf workflow"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])
        workflowTestUtil.pollAndCompleteTask('integration_task_2', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the new leaf workflow reached COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(newLeafWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }

        when: "the mid level and root workflows are 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "verify that the mid level and root workflows reach COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.COMPLETED
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.COMPLETED
            (!tasks[1].subworkflowChanged) // flag is reset after decide
        }
        //endregion

        cleanup: "Ensure that changes to the task def are reverted"
        metadataService.updateTaskDef(persistedTask2Definition)
    }

    /**
     * On a 3-level workflow where all workflows reach FAILED state because of a FAILED task
     * in the leaf workflow.
     *
     * A retry is executed with resume flag on the mid-level workflow.
     *
     * Expectation: The leaf workflow is retried and both its parent (mid-level) and grand parent (root) workflows are also retried.
     * When the leaf workflow completes successfully, both the mid-level and root workflows also complete successfully.
     */
    def "Test retry on the mid-level with resume flag in a 3-level subworkflow"() {
        //region Test setup: 3 workflows reach FAILED state. Task 'integration_task_2' in leaf workflow is FAILED.
        setup: "Modify task definition to 0 retries"
        def persistedTask2Definition = workflowTestUtil.getPersistedTaskDefinition('integration_task_2').get()
        def modifiedTask2Definition = new TaskDef(persistedTask2Definition.name, persistedTask2Definition.description,
                persistedTask2Definition.ownerEmail, 0, persistedTask2Definition.timeoutSeconds,
                persistedTask2Definition.responseTimeoutSeconds)
        metadataService.updateTaskDef(modifiedTask2Definition)

        and: "an existing workflow with subworkflow and registered definitions"
        metadataService.getWorkflowDef(SIMPLE_WORKFLOW, 1)
        metadataService.getWorkflowDef(WORKFLOW_WITH_SUBWORKFLOW, 1)

        and: "input required to start the workflow execution"
        String correlationId = 'retry_on_root_in_3level_wf'
        def input = [
                'param1'   : 'p1 value',
                'param2'   : 'p2 value',
                'subwf'    : WORKFLOW_WITH_SUBWORKFLOW,
                'nextSubwf': SIMPLE_WORKFLOW]

        when: "the workflow is started"
        def rootWorkflowId = workflowExecutor.startWorkflow(WORKFLOW_WITH_SUBWORKFLOW, 1,
                correlationId, input, null, null, null)

        then: "verify that the workflow is in a RUNNING state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and complete the integration_task_1 task"
        def pollAndCompleteTask = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the 'integration_task_1' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask)

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        List<String> polledTaskIds = queueDAO.pop("SUB_WORKFLOW", 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)

        then: "verify that the 'sub_workflow_task' is in a IN_PROGRESS state"
        def rootWorkflowInstance = workflowExecutionService.getExecutionStatus(rootWorkflowId, true)
        with(rootWorkflowInstance) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
        }

        and: "verify that the mid-level workflow is RUNNING, and first task is in SCHEDULED state"
        def midLevelWorkflowId = rootWorkflowInstance.tasks[1].subWorkflowId
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        and: "poll and complete the integration_task_1 task in the mid-level workflow"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        polledTaskIds = queueDAO.pop(TASK_TYPE_SUB_WORKFLOW, 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)
        def midLevelWorkflowInstance = workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)

        then: "verify that the mid-level workflow is RUNNING, and first task is in SCHEDULED state"
        def leafWorkflowId = midLevelWorkflowInstance.tasks[1].subWorkflowId
        def leafWorkflowInstance = workflowExecutionService.getExecutionStatus(leafWorkflowId, true)
        with(leafWorkflowInstance) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and fail the integration_task_2 task"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])
        workflowTestUtil.pollAndFailTask('integration_task_2', 'task2.integration.worker', 'failed')

        then: "the leaf workflow ends up in a FAILED state"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
        }

        when: "the mid level workflow is 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)

        then: "the mid level subworkflow is in FAILED state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
        }

        when: "the root level workflow is 'decided'"
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "the root level workflow is in FAILED state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
        }
        //endregion

        //region Test case
        when: "do a retry on the root workflow"
        workflowExecutor.retry(midLevelWorkflowId, true)

        then: "verify that the sub workflow task in root workflow is IN_PROGRESS state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            tasks[1].subworkflowChanged
        }

        and: "verify that the sub workflow task in mid level workflow is IN_PROGRESS state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            tasks[1].subworkflowChanged
        }

        and: "verify that the previously failed task in leaf workflow is in SCHEDULED state"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 3
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
            tasks[1].retried
            tasks[2].taskType == 'integration_task_2'
            tasks[2].status == Task.Status.SCHEDULED
        }

        when: "the mid level and root workflows are 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "the mid level workflow is in RUNNING state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            !tasks[1].subworkflowChanged // flag is reset after "decide"
        }

        and: "the root workflow is in RUNNING state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            !tasks[1].subworkflowChanged // flag is reset after "decide"
        }

        when: "poll and complete the previously failed integration_task_2 task"
        workflowTestUtil.pollAndCompleteTask('integration_task_2', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the leaf workflow is in COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }

        when: "the mid level and root workflows are 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "the new mid level workflow is in COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }

        and: "the root workflow is in COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }
        //endregion

        cleanup: "Ensure that changes to the task def are reverted"
        metadataService.updateTaskDef(persistedTask2Definition)
    }

    /**
     * On a 3-level workflow where all workflows reach FAILED state because of a FAILED task
     * in the leaf workflow.
     *
     * A retry is executed on the leaf workflow.
     *
     * Expectation: The leaf workflow resumes its FAILED task and updates both its parent (mid-level) and grandparent (root).
     * When the leaf workflow completes successfully, both the mid-level and root workflows also complete successfully.
     */
    def "Test retry on the leaf in a 3-level subworkflow"() {
        //region Test setup: 3 workflows reach FAILED state. Task 'integration_task_2' in leaf workflow is FAILED.
        setup: "Modify task definition to 0 retries"
        def persistedTask2Definition = workflowTestUtil.getPersistedTaskDefinition('integration_task_2').get()
        def modifiedTask2Definition = new TaskDef(persistedTask2Definition.name, persistedTask2Definition.description,
                persistedTask2Definition.ownerEmail, 0, persistedTask2Definition.timeoutSeconds,
                persistedTask2Definition.responseTimeoutSeconds)
        metadataService.updateTaskDef(modifiedTask2Definition)

        and: "an existing workflow with subworkflow and registered definitions"
        metadataService.getWorkflowDef(SIMPLE_WORKFLOW, 1)
        metadataService.getWorkflowDef(WORKFLOW_WITH_SUBWORKFLOW, 1)

        and: "input required to start the workflow execution"
        String correlationId = 'retry_on_root_in_3level_wf'
        def input = [
                'param1'   : 'p1 value',
                'param2'   : 'p2 value',
                'subwf'    : WORKFLOW_WITH_SUBWORKFLOW,
                'nextSubwf': SIMPLE_WORKFLOW]

        when: "the workflow is started"
        def rootWorkflowId = workflowExecutor.startWorkflow(WORKFLOW_WITH_SUBWORKFLOW, 1,
                correlationId, input, null, null, null)

        then: "verify that the workflow is in a RUNNING state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and complete the integration_task_1 task"
        def pollAndCompleteTask = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the 'integration_task_1' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask)

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        List<String> polledTaskIds = queueDAO.pop("SUB_WORKFLOW", 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)

        then: "verify that the 'sub_workflow_task' is in a IN_PROGRESS state"
        def rootWorkflowInstance = workflowExecutionService.getExecutionStatus(rootWorkflowId, true)
        with(rootWorkflowInstance) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
        }

        and: "verify that the mid-level workflow is RUNNING, and first task is in SCHEDULED state"
        def midLevelWorkflowId = rootWorkflowInstance.tasks[1].subWorkflowId
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        and: "poll and complete the integration_task_1 task in the mid-level workflow"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        polledTaskIds = queueDAO.pop(TASK_TYPE_SUB_WORKFLOW, 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)
        def midLevelWorkflowInstance = workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)

        then: "verify that the mid-level workflow is RUNNING, and first task is in SCHEDULED state"
        def leafWorkflowId = midLevelWorkflowInstance.tasks[1].subWorkflowId
        def leafWorkflowInstance = workflowExecutionService.getExecutionStatus(leafWorkflowId, true)
        with(leafWorkflowInstance) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and fail the integration_task_2 task"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])
        workflowTestUtil.pollAndFailTask('integration_task_2', 'task2.integration.worker', 'failed')

        then: "the leaf workflow ends up in a FAILED state"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
        }

        when: "the mid level workflow is 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)

        then: "the mid level subworkflow is in FAILED state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
        }

        when: "the root level workflow is 'decided'"
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "the root level workflow is in FAILED state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
        }
        //endregion

        //region Test case
        when: "do a retry on the leaf workflow"
        workflowExecutor.retry(leafWorkflowId, false)

        then: "verify that the leaf workflow is in RUNNING state and failed task is retried"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 3
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
            tasks[1].retried
            tasks[2].taskType == 'integration_task_2'
            tasks[2].status == Task.Status.SCHEDULED
            tasks[2].retriedTaskId == tasks[1].taskId
        }

        then: "verify that the mid-level workflow's SUB_WORKFLOW task is updated"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            tasks[1].subworkflowChanged
        }

        and: "verify that the root workflow's SUB_WORKFLOW task is updated"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            tasks[1].subworkflowChanged
        }

        and: "verify the SUB_WORKFLOW task in root workflow is IN_PROGRESS state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            tasks[1].subworkflowChanged
        }

        when: "poll and complete the scheduled task in the leaf workflow"
        workflowTestUtil.pollAndCompleteTask('integration_task_2', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the leaf workflow reached COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }

        when: "the mid level and root workflows are 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "verify that the mid level and root workflows reach COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.COMPLETED
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.COMPLETED
            (!tasks[1].subworkflowChanged) // flag is reset after decide
        }
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.COMPLETED
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.COMPLETED
            (!tasks[1].subworkflowChanged) // flag is reset after decide
        }
        //endregion

        cleanup: "Ensure that changes to the task def are reverted"
        metadataService.updateTaskDef(persistedTask2Definition)
    }

    /**
     * On a 3-level workflow where all workflows reach FAILED state because of a FAILED task
     * in the leaf workflow.
     *
     * A retry is executed with resume flag on the leaf workflow.
     *
     * Expectation: The leaf workflow resumes its FAILED task and updates both its parent (mid-level) and grandparent (root).
     * When the leaf workflow completes successfully, both the mid-level and root workflows also complete successfully.
     */
    def "Test retry on the leaf with resume flag in a 3-level subworkflow"() {
        //region Test setup: 3 workflows reach FAILED state. Task 'integration_task_2' in leaf workflow is FAILED.
        setup: "Modify task definition to 0 retries"
        def persistedTask2Definition = workflowTestUtil.getPersistedTaskDefinition('integration_task_2').get()
        def modifiedTask2Definition = new TaskDef(persistedTask2Definition.name, persistedTask2Definition.description,
                persistedTask2Definition.ownerEmail, 0, persistedTask2Definition.timeoutSeconds,
                persistedTask2Definition.responseTimeoutSeconds)
        metadataService.updateTaskDef(modifiedTask2Definition)

        and: "an existing workflow with subworkflow and registered definitions"
        metadataService.getWorkflowDef(SIMPLE_WORKFLOW, 1)
        metadataService.getWorkflowDef(WORKFLOW_WITH_SUBWORKFLOW, 1)

        and: "input required to start the workflow execution"
        String correlationId = 'retry_on_root_in_3level_wf'
        def input = [
                'param1'   : 'p1 value',
                'param2'   : 'p2 value',
                'subwf'    : WORKFLOW_WITH_SUBWORKFLOW,
                'nextSubwf': SIMPLE_WORKFLOW]

        when: "the workflow is started"
        def rootWorkflowId = workflowExecutor.startWorkflow(WORKFLOW_WITH_SUBWORKFLOW, 1,
                correlationId, input, null, null, null)

        then: "verify that the workflow is in a RUNNING state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and complete the integration_task_1 task"
        def pollAndCompleteTask = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the 'integration_task_1' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask)

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        List<String> polledTaskIds = queueDAO.pop("SUB_WORKFLOW", 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)

        then: "verify that the 'sub_workflow_task' is in a IN_PROGRESS state"
        def rootWorkflowInstance = workflowExecutionService.getExecutionStatus(rootWorkflowId, true)
        with(rootWorkflowInstance) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
        }

        and: "verify that the mid-level workflow is RUNNING, and first task is in SCHEDULED state"
        def midLevelWorkflowId = rootWorkflowInstance.tasks[1].subWorkflowId
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        and: "poll and complete the integration_task_1 task in the mid-level workflow"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        when: "the subworkflow task should be in SCHEDULED state and is started by issuing a system task call"
        polledTaskIds = queueDAO.pop(TASK_TYPE_SUB_WORKFLOW, 1, 200)
        workflowExecutor.executeSystemTask(WorkflowSystemTask.get(SubWorkflow.NAME), polledTaskIds.get(0), 30)
        def midLevelWorkflowInstance = workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)

        then: "verify that the mid-level workflow is RUNNING, and first task is in SCHEDULED state"
        def leafWorkflowId = midLevelWorkflowInstance.tasks[1].subWorkflowId
        def leafWorkflowInstance = workflowExecutionService.getExecutionStatus(leafWorkflowId, true)
        with(leafWorkflowInstance) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        when: "poll and fail the integration_task_2 task"
        workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])
        workflowTestUtil.pollAndFailTask('integration_task_2', 'task2.integration.worker', 'failed')

        then: "the leaf workflow ends up in a FAILED state"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
        }

        when: "the mid level workflow is 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)

        then: "the mid level subworkflow is in FAILED state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
        }

        when: "the root level workflow is 'decided'"
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "the root level workflow is in FAILED state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.FAILED
        }
        //endregion

        //region Test case
        when: "do a retry on the leaf workflow"
        workflowExecutor.retry(leafWorkflowId, true)

        then: "verify that the leaf workflow is in RUNNING state and failed task is retried"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 3
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.FAILED
            tasks[1].retried
            tasks[2].taskType == 'integration_task_2'
            tasks[2].status == Task.Status.SCHEDULED
            tasks[2].retriedTaskId == tasks[1].taskId
        }

        then: "verify that the mid-level workflow's SUB_WORKFLOW task is updated"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            tasks[1].subworkflowChanged
        }

        and: "verify that the root workflow's SUB_WORKFLOW task is updated"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            tasks[1].subworkflowChanged
        }

        and: "verify the SUB_WORKFLOW task in root workflow is IN_PROGRESS state"
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.IN_PROGRESS
            tasks[1].subworkflowChanged
        }

        when: "poll and complete the scheduled task in the leaf workflow"
        workflowTestUtil.pollAndCompleteTask('integration_task_2', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the leaf workflow reached COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(leafWorkflowId, false)) {
            status == Workflow.WorkflowStatus.COMPLETED
        }

        when: "the mid level and root workflows are 'decided'"
        workflowSweeper.sweep([midLevelWorkflowId], workflowExecutor, workflowRepairService)
        workflowSweeper.sweep([rootWorkflowId], workflowExecutor, workflowRepairService)

        then: "verify that the mid level and root workflows reach COMPLETED state"
        with(workflowExecutionService.getExecutionStatus(midLevelWorkflowId, true)) {
            status == Workflow.WorkflowStatus.COMPLETED
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.COMPLETED
            (!tasks[1].subworkflowChanged) // flag is reset after decide
        }
        with(workflowExecutionService.getExecutionStatus(rootWorkflowId, true)) {
            status == Workflow.WorkflowStatus.COMPLETED
            tasks.size() == 2
            tasks[1].taskType == TASK_TYPE_SUB_WORKFLOW
            tasks[1].status == Task.Status.COMPLETED
            (!tasks[1].subworkflowChanged) // flag is reset after decide
        }
        //endregion

        cleanup: "Ensure that changes to the task def are reverted"
        metadataService.updateTaskDef(persistedTask2Definition)
    }
}
