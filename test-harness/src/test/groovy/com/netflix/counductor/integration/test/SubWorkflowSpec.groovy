package com.netflix.counductor.integration.test

import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.run.Workflow
import com.netflix.conductor.core.execution.WorkflowExecutor
import com.netflix.conductor.core.execution.tasks.SystemTaskWorkerCoordinator
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask
import com.netflix.conductor.dao.QueueDAO
import com.netflix.conductor.service.ExecutionService
import com.netflix.conductor.service.MetadataService
import com.netflix.conductor.test.util.WorkflowTestUtil
import com.netflix.conductor.tests.utils.TestModule
import com.netflix.governator.guice.test.ModulesForTesting
import spock.lang.Shared
import spock.lang.Specification

import javax.inject.Inject

import static com.netflix.conductor.test.util.WorkflowTestUtil.verifyPolledAndAcknowledgedTask

@ModulesForTesting([TestModule.class])
class SubWorkflowSpec extends Specification{

    @Inject
    ExecutionService workflowExecutionService

    @Inject
    MetadataService metadataService

    @Inject
    WorkflowExecutor workflowExecutor

    @Inject
    WorkflowTestUtil workflowTestUtil

    @Inject
    QueueDAO queueDAO

    @Shared
    def WORKFLOW_WITH_SUBWORKFLOW = 'integration_test_wf_with_sub_wf'

    @Shared
    def SUB_WORKFLOW = "sub_workflow"


    def setup() {
        //Register SUB_WORKFLOW, WORKFLOW_WITH_SUBWORKFLOW
        workflowTestUtil.registerWorkflows('simple_one_task_sub_workflow_integration_test.json',
                'workflow_with_sub_workflow_1_integration_test.json')
    }

    def cleanup() {
        workflowTestUtil.clearWorkflows()
    }

    def "Test workflow with subworkflow completion"() {
        given: "Existing workflow and subworkflow definitions"
        metadataService.getWorkflowDef(SUB_WORKFLOW, 1)
        metadataService.getWorkflowDef(WORKFLOW_WITH_SUBWORKFLOW, 1)

        and: "input required to start the workflow execution"
        String correlationId = 'wf_with_subwf_test_1'
        def input = new HashMap()
        String inputParam1 = 'p1 value'
        input['param1'] = inputParam1
        input['param2'] = 'p2 value'

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
        List<String> polledTaskIds = queueDAO.pop("SUB_WORKFLOW", 1, 200);
        WorkflowSystemTask systemTask = SystemTaskWorkerCoordinator.taskNameWorkflowTaskMapping.get("SUB_WORKFLOW")
        workflowExecutor.executeSystemTask(systemTask, polledTaskIds.get(0), 30)
        def workflow = workflowExecutionService.getExecutionStatus(workflowInstanceId, true)

        then: "verify that the 'sub_workflow_task' is polled and IN_PROGRESS"
        workflow.getStatus() == Workflow.WorkflowStatus.RUNNING
        def tasks = workflow.getTasks()
        tasks.size() == 2
        tasks[0].taskType == 'integration_task_1'
        tasks[0].status == Task.Status.COMPLETED
        tasks[1].taskType == 'SUB_WORKFLOW'
        tasks[1].status == Task.Status.IN_PROGRESS

        when: "Checking Subworkflow created by above sub_workflow_task"
        def subWorkflowId = workflow.tasks[1].subWorkflowId
        def subWorkflow = workflowExecutionService.getExecutionStatus(subWorkflowId, true)

        then: "verify that the sub workflow is RUNNING, and first task is in SCHEDULED state"
        subWorkflow.getStatus() == Workflow.WorkflowStatus.RUNNING
        subWorkflow.getTasks().size() == 1
        subWorkflow.getTasks()[0].taskType == 'integration_task_1'
        subWorkflow.getTasks()[0].status == Task.Status.SCHEDULED

        when: "Polled for integration_task_1 task in subworkflow"
        pollAndCompleteTask1Try1 = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker', ['op': 'task1.done'])

        then: "verify that the 'integration_task_1' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask1Try1)

        when: "Checking Subworkflow created by above sub_workflow_task"
        subWorkflow = workflowExecutionService.getExecutionStatus(subWorkflowId, true)

        then: "verify that the 'integration_task1' is complete and the next task integration_task_2 is in scheduled state"
        subWorkflow.getStatus() == Workflow.WorkflowStatus.RUNNING
        subWorkflow.getTasks().size() == 2
        subWorkflow.getTasks()[0].taskType == 'integration_task_1'
        subWorkflow.getTasks()[0].status == Task.Status.COMPLETED
        subWorkflow.getTasks()[1].taskType == 'integration_task_2'
        subWorkflow.getTasks()[1].status == Task.Status.SCHEDULED

        when: "poll and complete 'integration_task_2'"
        def pollAndCompleteTask2Try1 = workflowTestUtil.pollAndCompleteTask('integration_task_2', 'task2.integration.worker')

        then: "verify that the 'integration_task_2' has been polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask2Try1, ['tp1': inputParam1, 'tp2': 'task1.done'])

        when: "Checking Subworkflow created by above sub_workflow_task"
        subWorkflow = workflowExecutionService.getExecutionStatus(subWorkflowId, true)

        then: "verify that the subworkflow is in a completed state"
        subWorkflow.getStatus() == Workflow.WorkflowStatus.COMPLETED
        subWorkflow.getTasks().size() == 2
        subWorkflow.getTasks()[1].taskType == 'integration_task_2'
        subWorkflow.getTasks()[1].status == Task.Status.COMPLETED
        subWorkflow.output.containsKey('o3')

        when: "Checking workflow final state"
        workflow = workflowExecutionService.getExecutionStatus(workflowInstanceId, true)

        then: "verify that the workflow is in a completed state"
        workflow.status == Workflow.WorkflowStatus.COMPLETED
        workflow.tasks.size() == 2
        workflow.tasks[1].taskType == 'SUB_WORKFLOW'
        workflow.tasks[1].status == Task.Status.COMPLETED
        workflow.output.containsKey('o3')
    }

    def "Test workflow with subworkflow failure and retry"() {
        given: "input required to start the workflow execution"
        String correlationId = 'wf_with_subwf_test_1'
        def input = new HashMap()
        String inputParam1 = 'p1 value'
        input['param1'] = inputParam1
        input['param2'] = 'p2 value'

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
        List<String> polledTaskIds = queueDAO.pop("SUB_WORKFLOW", 1, 200);
        WorkflowSystemTask systemTask = SystemTaskWorkerCoordinator.taskNameWorkflowTaskMapping.get("SUB_WORKFLOW")
        workflowExecutor.executeSystemTask(systemTask, polledTaskIds.get(0), 30)
        def workflow = workflowExecutionService.getExecutionStatus(workflowInstanceId, true)

        then: "verify that the 'sub_workflow_task' is polled and IN_PROGRESS"
        workflow.getStatus() == Workflow.WorkflowStatus.RUNNING
        def tasks = workflow.getTasks()
        tasks.size() == 2
        tasks[0].taskType == 'integration_task_1'
        tasks[0].status == Task.Status.COMPLETED
        tasks[1].taskType == 'SUB_WORKFLOW'
        tasks[1].status == Task.Status.IN_PROGRESS

        when: "Checking Subworkflow created by above sub_workflow_task"
        def subWorkflowId = workflow.tasks[1].subWorkflowId
        def subWorkflow = workflowExecutionService.getExecutionStatus(subWorkflowId, true)

        then: "verify that the sub workflow is RUNNING, and first task is in SCHEDULED state"
        subWorkflow.getStatus() == Workflow.WorkflowStatus.RUNNING
        subWorkflow.getTasks().size() == 1
        subWorkflow.getTasks()[0].taskType == 'simple_task_in_sub_wf'
        subWorkflow.getTasks()[0].status == Task.Status.SCHEDULED

        when: "Polled for simple_task_in_sub_wf task in subworkflow"
        pollAndCompleteTask1Try1 = workflowTestUtil.pollAndFailTask('simple_task_in_sub_wf', 'task1.integration.worker', 'Failing a task in sub workflow', ['op': 'task1.done'])

        then: "verify that the 'simple_task_in_sub_wf' was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(pollAndCompleteTask1Try1)

        when: "Checking Subworkflow created by above sub_workflow_task"
        subWorkflow = workflowExecutionService.getExecutionStatus(subWorkflowId, true)

        then: "verify that the 'simple_task_in_sub_wf' is failed and retried"
        subWorkflow.getStatus() == Workflow.WorkflowStatus.FAILED
        subWorkflow.getTasks().size() == 1
        subWorkflow.getTasks()[0].taskType == 'simple_task_in_sub_wf'
        subWorkflow.getTasks()[0].status == Task.Status.FAILED

        when: "Checking workflow final state"
        workflow = workflowExecutionService.getExecutionStatus(workflowInstanceId, true)

        then: "verify that the workflow is in a failed state"
        workflow.status == Workflow.WorkflowStatus.FAILED
        workflow.tasks.size() == 2
        workflow.tasks[1].taskType == 'SUB_WORKFLOW'
        workflow.tasks[1].status == Task.Status.FAILED
        workflow.output.containsKey('op')

        when: "Failed workflow is retried"
        workflowExecutor.retry(subWorkflowId)
        subWorkflow = workflowExecutionService.getExecutionStatus(subWorkflowId, true)
        workflow = workflowExecutionService.getExecutionStatus(workflowInstanceId, true)

        then: "verify that both subworkflow and parent workflow are in RUNNING state after retry"
        subWorkflow.getStatus() == Workflow.WorkflowStatus.RUNNING
        subWorkflow.getTasks().size() == 2
        subWorkflow.getTasks()[0].taskType == 'simple_task_in_sub_wf'
        subWorkflow.getTasks()[0].status == Task.Status.FAILED
        subWorkflow.getTasks()[1].taskType == 'simple_task_in_sub_wf'
        subWorkflow.getTasks()[1].status == Task.Status.SCHEDULED

        workflow.getStatus() == Workflow.WorkflowStatus.RUNNING
        workflow.getTasks().size() == 2
        workflow.getTasks()[0].taskType == 'integration_task_1'
        workflow.getTasks()[0].status == Task.Status.COMPLETED
        workflow.getTasks()[1].taskType == 'SUB_WORKFLOW'
        workflow.getTasks()[1].status == Task.Status.IN_PROGRESS
    }
}
