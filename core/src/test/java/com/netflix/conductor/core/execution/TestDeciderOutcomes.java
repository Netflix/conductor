/**
 * Copyright 2016 Netflix, Inc.
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
/**
 * 
 */
package com.netflix.conductor.core.execution;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask.Type;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.DeciderService.DeciderOutcome;
import com.netflix.conductor.core.execution.mapper.DecisionTaskMapper;
import com.netflix.conductor.core.execution.mapper.DynamicTaskMapper;
import com.netflix.conductor.core.execution.mapper.EventTaskMapper;
import com.netflix.conductor.core.execution.mapper.ForkJoinDynamicTaskMapper;
import com.netflix.conductor.core.execution.mapper.ForkJoinTaskMapper;
import com.netflix.conductor.core.execution.mapper.JoinTaskMapper;
import com.netflix.conductor.core.execution.mapper.SimpleTaskMapper;
import com.netflix.conductor.core.execution.mapper.SubWorkflowTaskMapper;
import com.netflix.conductor.core.execution.mapper.TaskMapper;
import com.netflix.conductor.core.execution.mapper.UserDefinedTaskMapper;
import com.netflix.conductor.core.execution.mapper.WaitTaskMapper;
import com.netflix.conductor.core.execution.tasks.Join;
import com.netflix.conductor.dao.MetadataDAO;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Viren
 *
 */
public class TestDeciderOutcomes {

	private DeciderService ds;
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	static {
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        objectMapper.setSerializationInclusion(Include.NON_EMPTY);
	}
	
	
	@Before
	public void init() throws Exception {
		
		MetadataDAO metadataDAO = mock(MetadataDAO.class);
		TaskDef td = new TaskDef();
		td.setRetryCount(1);
		when(metadataDAO.getTaskDef(any())).thenReturn(td);
		ParametersUtils parametersUtils = new ParametersUtils();
		Map<String, TaskMapper> taskMappers = new HashMap<>();
		taskMappers.put("DECISION", new DecisionTaskMapper());
		taskMappers.put("DYNAMIC", new DynamicTaskMapper(parametersUtils, metadataDAO));
		taskMappers.put("FORK_JOIN", new ForkJoinTaskMapper());
		taskMappers.put("JOIN", new JoinTaskMapper());
		taskMappers.put("FORK_JOIN_DYNAMIC", new ForkJoinDynamicTaskMapper(parametersUtils, objectMapper));
		taskMappers.put("USER_DEFINED", new UserDefinedTaskMapper(parametersUtils, metadataDAO));
		taskMappers.put("SIMPLE", new SimpleTaskMapper(parametersUtils, metadataDAO));
		taskMappers.put("SUB_WORKFLOW", new SubWorkflowTaskMapper(parametersUtils, metadataDAO));
		taskMappers.put("EVENT", new EventTaskMapper(parametersUtils));
		taskMappers.put("WAIT", new WaitTaskMapper(parametersUtils));

		this.ds = new DeciderService(metadataDAO, taskMappers);
	}
	
	@Test
	public void testWorkflowWithNoTasks() throws Exception {
		InputStream stream = TestDeciderOutcomes.class.getResourceAsStream("/conditional_flow.json");
		WorkflowDef def = objectMapper.readValue(stream, WorkflowDef.class);
		assertNotNull(def);
		
		Workflow workflow = new Workflow();
		workflow.setWorkflowType(def.getName());
		workflow.setStartTime(0);
		workflow.getInput().put("param1", "nested");
		workflow.getInput().put("param2", "one");
		
		DeciderOutcome outcome = ds.decide(workflow, def);
		assertNotNull(outcome);
		assertFalse(outcome.isComplete);
		assertTrue(outcome.tasksToBeUpdated.isEmpty());
		assertEquals(3, outcome.tasksToBeScheduled.size());
		System.out.println(outcome.tasksToBeScheduled);
		
		outcome.tasksToBeScheduled.forEach(t -> t.setStatus(Status.COMPLETED));
		workflow.getTasks().addAll(outcome.tasksToBeScheduled);
		outcome = ds.decide(workflow, def);
		assertFalse(outcome.isComplete);
		assertEquals(outcome.tasksToBeUpdated.toString(), 3, outcome.tasksToBeUpdated.size());
		assertEquals(1, outcome.tasksToBeScheduled.size());
		assertEquals("junit_task_3", outcome.tasksToBeScheduled.get(0).getTaskDefName());
		System.out.println(outcome.tasksToBeScheduled);
	}
	
	
	@Test
	public void testRetries() {
		WorkflowDef def = new WorkflowDef();
		def.setName("test");
		
		WorkflowTask task = new WorkflowTask();
		task.setName("test_task");
		task.setType("USER_TASK");
		task.setTaskReferenceName("t0");
		task.getInputParameters().put("taskId", "${CPEWF_TASK_ID}");
		task.getInputParameters().put("requestId", "${workflow.input.requestId}");
		
		def.getTasks().add(task);
		def.setSchemaVersion(2);
		
		Workflow workflow = new Workflow();
		workflow.getInput().put("requestId", 123);
		workflow.setStartTime(System.currentTimeMillis());
		DeciderOutcome outcome = ds.decide(workflow, def);
		assertNotNull(outcome);
		
		assertEquals(1, outcome.tasksToBeScheduled.size());
		assertEquals(task.getTaskReferenceName(), outcome.tasksToBeScheduled.get(0).getReferenceTaskName());
		
		String task1Id = outcome.tasksToBeScheduled.get(0).getTaskId();
		assertEquals(task1Id, outcome.tasksToBeScheduled.get(0).getInputData().get("taskId"));
		assertEquals(123, outcome.tasksToBeScheduled.get(0).getInputData().get("requestId"));
		
		outcome.tasksToBeScheduled.get(0).setStatus(Status.FAILED);
		workflow.getTasks().addAll(outcome.tasksToBeScheduled);

		outcome = ds.decide(workflow, def);
		assertNotNull(outcome);
		
		assertEquals(1, outcome.tasksToBeUpdated.size());
		assertEquals(1, outcome.tasksToBeScheduled.size());
		assertEquals(task1Id, outcome.tasksToBeUpdated.get(0).getTaskId());
		assertNotSame(task1Id, outcome.tasksToBeScheduled.get(0).getTaskId());
		assertEquals(outcome.tasksToBeScheduled.get(0).getTaskId(), outcome.tasksToBeScheduled.get(0).getInputData().get("taskId"));
		assertEquals(task1Id, outcome.tasksToBeScheduled.get(0).getRetriedTaskId());		
		assertEquals(123, outcome.tasksToBeScheduled.get(0).getInputData().get("requestId"));
		
		
		WorkflowTask fork = new WorkflowTask();
		fork.setName("fork0");
		fork.setWorkflowTaskType(Type.FORK_JOIN_DYNAMIC);
		fork.setTaskReferenceName("fork0");
		fork.setDynamicForkTasksInputParamName("forkedInputs");
		fork.setDynamicForkTasksParam("forks");
		fork.getInputParameters().put("forks", "${workflow.input.forks}");
		fork.getInputParameters().put("forkedInputs", "${workflow.input.forkedInputs}");
		
		WorkflowTask join = new WorkflowTask();
		join.setName("join0");
		join.setType("JOIN");
		join.setTaskReferenceName("join0");
		
		def.getTasks().clear();
		def.getTasks().add(fork);
		def.getTasks().add(join);
		
		List<WorkflowTask> forks = new LinkedList<>();
		Map<String, Map<String, Object>> forkedInputs = new HashMap<>();
		
		for(int i = 0; i < 1; i++) {
			WorkflowTask wft = new WorkflowTask();
			wft.setName("f" + i);
			wft.setTaskReferenceName("f" + i);
			wft.setWorkflowTaskType(Type.SIMPLE);
			wft.getInputParameters().put("requestId", "${workflow.input.requestId}");
			wft.getInputParameters().put("taskId", "${CPEWF_TASK_ID}");
			forks.add(wft);
			Map<String, Object> input = new HashMap<>();
			input.put("k", "v");
			input.put("k1", 1);
			forkedInputs.put(wft.getTaskReferenceName(), input);
		}
		workflow = new Workflow();
		workflow.getInput().put("requestId", 123);
		workflow.setStartTime(System.currentTimeMillis());
		
		workflow.getInput().put("forks", forks);
		workflow.getInput().put("forkedInputs", forkedInputs);
		
		outcome = ds.decide(workflow, def);
		assertNotNull(outcome);
		assertEquals(3, outcome.tasksToBeScheduled.size());
		assertEquals(0, outcome.tasksToBeUpdated.size());

		assertEquals("v", outcome.tasksToBeScheduled.get(1).getInputData().get("k"));
		assertEquals(1, outcome.tasksToBeScheduled.get(1).getInputData().get("k1"));
		assertEquals(outcome.tasksToBeScheduled.get(1).getTaskId(), outcome.tasksToBeScheduled.get(1).getInputData().get("taskId"));
		System.out.println(outcome.tasksToBeScheduled.get(1).getInputData());
		task1Id = outcome.tasksToBeScheduled.get(1).getTaskId();
		
		outcome.tasksToBeScheduled.get(1).setStatus(Status.FAILED);
		workflow.getTasks().addAll(outcome.tasksToBeScheduled);

		outcome = ds.decide(workflow, def);
		assertEquals("v", outcome.tasksToBeScheduled.get(1).getInputData().get("k"));
		assertEquals(1, outcome.tasksToBeScheduled.get(1).getInputData().get("k1"));
		assertEquals(outcome.tasksToBeScheduled.get(1).getTaskId(), outcome.tasksToBeScheduled.get(1).getInputData().get("taskId"));
		assertNotSame(task1Id, outcome.tasksToBeScheduled.get(1).getTaskId());
		assertEquals(task1Id, outcome.tasksToBeScheduled.get(1).getRetriedTaskId());
		System.out.println(outcome.tasksToBeScheduled.get(1).getInputData());

	}
	
	@Test
	public void testOptional() {
		WorkflowDef def = new WorkflowDef();
		def.setName("test");
		
		WorkflowTask task1 = new WorkflowTask();
		task1.setName("task0");
		task1.setType("SIMPLE");
		task1.setTaskReferenceName("t0");
		task1.getInputParameters().put("taskId", "${CPEWF_TASK_ID}");
		task1.setOptional(true);
		
		WorkflowTask task2 = new WorkflowTask();
		task2.setName("task1");
		task2.setType("SIMPLE");
		task2.setTaskReferenceName("t1");
		
		def.getTasks().add(task1);
		def.getTasks().add(task2);
		def.setSchemaVersion(2);
		
		
		Workflow workflow = new Workflow();
		workflow.setStartTime(System.currentTimeMillis());
		DeciderOutcome outcome = ds.decide(workflow, def);
		assertNotNull(outcome);
		
		System.out.println("Schedule after starting: " + outcome.tasksToBeScheduled);
		assertEquals(1, outcome.tasksToBeScheduled.size());
		assertEquals(task1.getTaskReferenceName(), outcome.tasksToBeScheduled.get(0).getReferenceTaskName());
		System.out.println("TaskId of the scheduled task in input: " + outcome.tasksToBeScheduled.get(0).getInputData());
		String task1Id = outcome.tasksToBeScheduled.get(0).getTaskId();
		assertEquals(task1Id, outcome.tasksToBeScheduled.get(0).getInputData().get("taskId"));
		
		workflow.getTasks().addAll(outcome.tasksToBeScheduled);
		workflow.getTasks().get(0).setStatus(Status.FAILED);
		
		outcome = ds.decide(workflow, def);
		
		assertNotNull(outcome);
		System.out.println("Schedule: " + outcome.tasksToBeScheduled);
		System.out.println("Update: " + outcome.tasksToBeUpdated);
		
		assertEquals(1, outcome.tasksToBeUpdated.size());
		assertEquals(1, outcome.tasksToBeScheduled.size());
		
		assertEquals(Task.Status.COMPLETED_WITH_ERRORS, workflow.getTasks().get(0).getStatus());
		assertEquals(task1Id, outcome.tasksToBeUpdated.get(0).getTaskId());
		assertEquals(task2.getTaskReferenceName(), outcome.tasksToBeScheduled.get(0).getReferenceTaskName());
		
	}
	
	@Test
	public void testOptionalWithDyammicFork() throws Exception {
		WorkflowDef def = new WorkflowDef();
		def.setName("test");
		
		WorkflowTask task1 = new WorkflowTask();
		task1.setName("fork0");
		task1.setWorkflowTaskType(Type.FORK_JOIN_DYNAMIC);
		task1.setTaskReferenceName("fork0");
		task1.setDynamicForkTasksInputParamName("forkedInputs");
		task1.setDynamicForkTasksParam("forks");
		task1.getInputParameters().put("forks", "${workflow.input.forks}");
		task1.getInputParameters().put("forkedInputs", "${workflow.input.forkedInputs}");
		
		WorkflowTask task2 = new WorkflowTask();
		task2.setName("join0");
		task2.setType("JOIN");
		task2.setTaskReferenceName("join0");
		
		def.getTasks().add(task1);
		def.getTasks().add(task2);
		def.setSchemaVersion(2);
		
		
		Workflow workflow = new Workflow();
		List<WorkflowTask> forks = new LinkedList<>();
		Map<String, Map<String, Object>> forkedInputs = new HashMap<>();
		
		for(int i = 0; i < 3; i++) {
			WorkflowTask wft = new WorkflowTask();
			wft.setName("f" + i);
			wft.setTaskReferenceName("f" + i);
			wft.setWorkflowTaskType(Type.SIMPLE);
			wft.setOptional(true);
			forks.add(wft);
			
			forkedInputs.put(wft.getTaskReferenceName(), new HashMap<>());
		}
		workflow.getInput().put("forks", forks);
		workflow.getInput().put("forkedInputs", forkedInputs);
		
		
		workflow.setStartTime(System.currentTimeMillis());
		DeciderOutcome outcome = ds.decide(workflow, def);
		assertNotNull(outcome);
		assertEquals(5, outcome.tasksToBeScheduled.size());
		assertEquals(0, outcome.tasksToBeUpdated.size());

		assertEquals(SystemTaskType.FORK.name(), outcome.tasksToBeScheduled.get(0).getTaskType());		
		assertEquals(Task.Status.COMPLETED, outcome.tasksToBeScheduled.get(0).getStatus());
		for(int i = 1; i < 4; i++) {
			assertEquals(Task.Status.SCHEDULED, outcome.tasksToBeScheduled.get(i).getStatus());
			assertEquals("f"+ (i-1), outcome.tasksToBeScheduled.get(i).getTaskDefName());			
			outcome.tasksToBeScheduled.get(i).setStatus(Status.FAILED);		//let's mark them as failure
		}
		assertEquals(Task.Status.IN_PROGRESS, outcome.tasksToBeScheduled.get(4).getStatus());
		workflow.getTasks().clear();		
		workflow.getTasks().addAll(outcome.tasksToBeScheduled);
		
		outcome = ds.decide(workflow, def);
		assertNotNull(outcome);
		assertEquals(SystemTaskType.JOIN.name(), outcome.tasksToBeScheduled.get(0).getTaskType());
		for(int i = 1; i < 4; i++) {
			assertEquals(Task.Status.COMPLETED_WITH_ERRORS, outcome.tasksToBeUpdated.get(i).getStatus());
			assertEquals("f"+ (i-1), outcome.tasksToBeUpdated.get(i).getTaskDefName());			
		}
		assertEquals(Task.Status.IN_PROGRESS, outcome.tasksToBeScheduled.get(0).getStatus());
		new Join().execute(workflow, outcome.tasksToBeScheduled.get(0), null);
		assertEquals(Task.Status.COMPLETED, outcome.tasksToBeScheduled.get(0).getStatus());
		
		outcome.tasksToBeScheduled.stream().map(task -> task.getStatus() + ":" + task.getTaskType() + ":").forEach(System.out::println);
		outcome.tasksToBeUpdated.stream().map(task -> task.getStatus() + ":" + task.getTaskType() + ":").forEach(System.out::println);
	}
	
	@Test
	public void testDecisionCases() {
		WorkflowDef def = new WorkflowDef();
		def.setName("test");
		
		WorkflowTask even = new WorkflowTask();
		even.setName("even");
		even.setType("SIMPLE");
		even.setTaskReferenceName("even");
		
		WorkflowTask odd = new WorkflowTask();
		odd.setName("odd");
		odd.setType("SIMPLE");
		odd.setTaskReferenceName("odd");
		
		WorkflowTask defaultt = new WorkflowTask();
		defaultt.setName("defaultt");
		defaultt.setType("SIMPLE");
		defaultt.setTaskReferenceName("defaultt");
		
		
		WorkflowTask decide = new WorkflowTask();
		decide.setName("decide");
		decide.setWorkflowTaskType(Type.DECISION);
		decide.setTaskReferenceName("d0");
		decide.getInputParameters().put("Id", "${workflow.input.Id}");
		decide.getInputParameters().put("location", "${workflow.input.location}");
		decide.setCaseExpression("if ($.Id == null) 'bad input'; else if ( ($.Id != null && $.Id % 2 == 0) || $.location == 'usa') 'even'; else 'odd'; ");
		
		decide.getDecisionCases().put("even", Arrays.asList(even));
		decide.getDecisionCases().put("odd", Arrays.asList(odd));
		decide.setDefaultCase(Arrays.asList(defaultt));
		
		def.getTasks().add(decide);
		def.setSchemaVersion(2);
		
		
		Workflow workflow = new Workflow();
		workflow.setStartTime(System.currentTimeMillis());
		DeciderOutcome outcome = ds.decide(workflow, def);
		assertNotNull(outcome);
		
		System.out.println("Schedule after starting: " + outcome.tasksToBeScheduled);
		assertEquals(2, outcome.tasksToBeScheduled.size());
		assertEquals(decide.getTaskReferenceName(), outcome.tasksToBeScheduled.get(0).getReferenceTaskName());
		assertEquals(defaultt.getTaskReferenceName(), outcome.tasksToBeScheduled.get(1).getReferenceTaskName());		//default
		System.out.println(outcome.tasksToBeScheduled.get(0).getOutputData().get("caseOutput"));
		assertEquals(Arrays.asList("bad input"), outcome.tasksToBeScheduled.get(0).getOutputData().get("caseOutput"));
		
		workflow.getInput().put("Id", 9);		
		workflow.getInput().put("location", "usa");
		outcome = ds.decide(workflow, def);
		assertEquals(2, outcome.tasksToBeScheduled.size());
		assertEquals(decide.getTaskReferenceName(), outcome.tasksToBeScheduled.get(0).getReferenceTaskName());
		assertEquals(even.getTaskReferenceName(), outcome.tasksToBeScheduled.get(1).getReferenceTaskName());		//even because of location == usa
		assertEquals(Arrays.asList("even"), outcome.tasksToBeScheduled.get(0).getOutputData().get("caseOutput"));
		
		workflow.getInput().put("Id", 9);		
		workflow.getInput().put("location", "canada");
		outcome = ds.decide(workflow, def);
		assertEquals(2, outcome.tasksToBeScheduled.size());
		assertEquals(decide.getTaskReferenceName(), outcome.tasksToBeScheduled.get(0).getReferenceTaskName());
		assertEquals(odd.getTaskReferenceName(), outcome.tasksToBeScheduled.get(1).getReferenceTaskName());			//odd
		assertEquals(Arrays.asList("odd"), outcome.tasksToBeScheduled.get(0).getOutputData().get("caseOutput"));
	}
	
}
