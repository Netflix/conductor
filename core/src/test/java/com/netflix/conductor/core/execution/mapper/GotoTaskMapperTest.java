package com.netflix.conductor.core.execution.mapper;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.DeciderService;
import com.netflix.conductor.core.execution.SystemTaskType;
import com.netflix.conductor.core.execution.TerminateWorkflowException;
import com.netflix.conductor.core.utils.IDGenerator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class GotoTaskMapperTest {

	private DeciderService deciderService;

	private GotoTaskMapper gotoTaskMapper;

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Before
	public void setUp() throws Exception {
		deciderService = Mockito.mock(DeciderService.class);
		gotoTaskMapper = new GotoTaskMapper();
	}

	@Test
	public void getMappedTasks() throws Exception {

		WorkflowDef def = new WorkflowDef();
		def.setName("GOTO_WF");
		def.setDescription(def.getName());
		def.setVersion(1);
		def.setInputParameters(Arrays.asList("param1", "param2"));

		WorkflowTask wft1 = new WorkflowTask();
		wft1.setName("junit_task_1");
		Map<String, Object> ip1 = new HashMap<>();
		ip1.put("p1", "workflow.input.param1");
		ip1.put("p2", "workflow.input.param2");
		wft1.setInputParameters(ip1);
		wft1.setTaskReferenceName("t1");

		WorkflowTask wft2 = new WorkflowTask();
		wft2.setName("junit_task_2");
		wft2.setInputParameters(ip1);
		wft2.setTaskReferenceName("t2");
		wft2.setInputParameters(ip1);

		WorkflowTask wGotoTask = new WorkflowTask();
		Map<String, Object> ip2 = new HashMap<>();
		ip2.put("p1", "workflow.input.param1_newvalue");
		wGotoTask.setInputParameters(ip2);
		wGotoTask.setType(TaskType.GOTO.name());
		wGotoTask.setName("gototask");
		wGotoTask.setTaskReferenceName("junit_gototask");
		wGotoTask.setGotoTask("t1");

		List<WorkflowTask> wTaskList = Arrays.asList(wft1, wft2, wGotoTask);
		def.getTasks().addAll(wTaskList);

		Workflow workflow = new Workflow();
		workflow.setWorkflowDefinition(def);

		Task task1 = new Task();
		task1.setReferenceTaskName(wft1.getTaskReferenceName());
		task1.setInputData(wft1.getInputParameters());

		Task task2 = new Task();
		task2.setReferenceTaskName(wft2.getTaskReferenceName());
		task2.setInputData(wft2.getInputParameters());

		Task gotoTask = new Task();
		gotoTask.setReferenceTaskName(wGotoTask.getTaskReferenceName());
		gotoTask.setInputData(wGotoTask.getInputParameters());

		Mockito.when(deciderService.getTasksToBeScheduled(workflow, wft1, 0, 0)).thenReturn(Arrays.asList(task1));
		Mockito.when(deciderService.getTasksToBeScheduled(workflow, wft2, 0, 0)).thenReturn(Arrays.asList(task2));
		Mockito.when(deciderService.getTasksToBeScheduled(workflow, wft1, 0, null, 0)).thenReturn(Arrays.asList(task1));
		Mockito.when(deciderService.getTasksToBeScheduled(workflow, wft1, 0, null, 1)).thenReturn(Arrays.asList(task1));
        

		String taskId = IDGenerator.generate();
		TaskMapperContext taskMapperContext = TaskMapperContext.newBuilder().withWorkflowDefinition(def)
				.withWorkflowInstance(workflow).withTaskToSchedule(wGotoTask).withRetryCount(0).withTaskInput(ip2)
				.withIterationCount(0).withTaskId(taskId).withDeciderService(deciderService).build();

		List<Task> mappedTasks = gotoTaskMapper.getMappedTasks(taskMapperContext);
        
		assertEquals(2, mappedTasks.size());
		assertEquals(wft1.getTaskReferenceName(), mappedTasks.get(1).getReferenceTaskName());
		assertEquals(mappedTasks.get(1).getInputData().get("p1"), "workflow.input.param1_newvalue");

	}

}
