package com.netflix.conductor.tests.integration;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.conductor.bootstrap.BootstrapModule;
import com.netflix.conductor.bootstrap.ModulesProvider;
import com.netflix.conductor.client.http.MetadataClient;
import com.netflix.conductor.client.http.TaskClient;
import com.netflix.conductor.client.http.WorkflowClient;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearch;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearchProvider;
import com.netflix.conductor.jetty.server.JettyServer;
import com.netflix.conductor.tests.integration.model.TaskWrapper;
import com.netflix.conductor.tests.utils.JsonUtils;
import com.netflix.conductor.tests.utils.TestEnvironment;

public class ExclusiveJoinEndToEndTest {

	private static TaskClient taskClient;

	private static WorkflowClient workflowClient;

	private static MetadataClient metadataClient;

	private static EmbeddedElasticSearch search;

	private static final int SERVER_PORT = 8080;

	private static String CONDUCTOR_URI = "http://localhost:8080/api/";

	private static String CONDUCTOR_WORKFLOW_DEF_NAME = "ExclusiveJoinTestWorkflow";

	private static Map<String, Object> workflowInput = new HashMap<>();

	private static Map<String, Object> taskOutput = new HashMap<>();

	@BeforeClass
	public static void setUp() throws Exception {
		TestEnvironment.setup();

		Injector bootInjector = Guice.createInjector(new BootstrapModule());
		Injector serverInjector = Guice.createInjector(bootInjector.getInstance(ModulesProvider.class).get());

		search = serverInjector.getInstance(EmbeddedElasticSearchProvider.class).get().get();
		search.start();

		JettyServer server = new JettyServer(SERVER_PORT, false);
		server.start();

		taskClient = new TaskClient();
		taskClient.setRootURI(CONDUCTOR_URI);
		workflowClient = new WorkflowClient();
		workflowClient.setRootURI(CONDUCTOR_URI);
		metadataClient = new MetadataClient();
		metadataClient.setRootURI(CONDUCTOR_URI);
	}

	@Before
	public void registerWorkflows() throws Exception {
		registerWorkflowDefinitions();
	}

	@Test
	public void testDecision1Default() throws Exception {
		workflowInput.put("decision_1", "null");

		StartWorkflowRequest startWorkflowRequest = new StartWorkflowRequest().withName(CONDUCTOR_WORKFLOW_DEF_NAME)
				.withCorrelationId("").withInput(workflowInput).withVersion(1);
		String wfInstanceId = workflowClient.startWorkflow(startWorkflowRequest);

		String taskId = taskClient.getPendingTaskForWorkflow(wfInstanceId, "task1").getTaskId();
		taskOutput.put("taskReferenceName", "task1");
		TaskResult taskResult = setTaskResult(wfInstanceId, taskId, TaskResult.Status.COMPLETED, taskOutput);
		taskClient.updateTask(taskResult, "");

		Workflow workflow = workflowClient.getWorkflow(wfInstanceId, true);
		String taskReferenceName = workflow.getTaskByRefName("exclusiveJoin").getOutputData().get("taskReferenceName")
				.toString();

		assertEquals("task1", taskReferenceName);
		assertEquals(Workflow.WorkflowStatus.COMPLETED, workflow.getStatus());
	}

	@Test
	public void testDecision1TrueAndDecision2Default() throws Exception {
		workflowInput.put("decision_1", "true");
		workflowInput.put("decision_2", "null");

		StartWorkflowRequest startWorkflowRequest = new StartWorkflowRequest().withName(CONDUCTOR_WORKFLOW_DEF_NAME)
				.withCorrelationId("").withInput(workflowInput).withVersion(1);
		String wfInstanceId = workflowClient.startWorkflow(startWorkflowRequest);

		String taskId = taskClient.getPendingTaskForWorkflow(wfInstanceId, "task1").getTaskId();
		taskOutput.put("taskReferenceName", "task1");
		TaskResult taskResult = setTaskResult(wfInstanceId, taskId, TaskResult.Status.COMPLETED, taskOutput);
		taskClient.updateTask(taskResult, "");

		taskId = taskClient.getPendingTaskForWorkflow(wfInstanceId, "task2").getTaskId();
		taskOutput.put("taskReferenceName", "task2");
		taskResult = setTaskResult(wfInstanceId, taskId, TaskResult.Status.COMPLETED, taskOutput);
		taskClient.updateTask(taskResult, "");

		Workflow workflow = workflowClient.getWorkflow(wfInstanceId, true);
		String taskReferenceName = workflow.getTaskByRefName("exclusiveJoin").getOutputData().get("taskReferenceName")
				.toString();

		assertEquals("task2", taskReferenceName);
		assertEquals(Workflow.WorkflowStatus.COMPLETED, workflow.getStatus());
	}

	@Test
	public void testDecision1TrueAndDecision2True() throws Exception {
		workflowInput.put("decision_1", "true");
		workflowInput.put("decision_2", "true");

		StartWorkflowRequest startWorkflowRequest = new StartWorkflowRequest().withName(CONDUCTOR_WORKFLOW_DEF_NAME)
				.withCorrelationId("").withInput(workflowInput).withVersion(1);
		String wfInstanceId = workflowClient.startWorkflow(startWorkflowRequest);

		String taskId = taskClient.getPendingTaskForWorkflow(wfInstanceId, "task1").getTaskId();
		taskOutput.put("taskReferenceName", "task1");
		TaskResult taskResult = setTaskResult(wfInstanceId, taskId, TaskResult.Status.COMPLETED, taskOutput);
		taskClient.updateTask(taskResult, "");

		taskId = taskClient.getPendingTaskForWorkflow(wfInstanceId, "task2").getTaskId();
		taskOutput.put("taskReferenceName", "task2");
		taskResult = setTaskResult(wfInstanceId, taskId, TaskResult.Status.COMPLETED, taskOutput);
		taskClient.updateTask(taskResult, "");

		taskId = taskClient.getPendingTaskForWorkflow(wfInstanceId, "task3").getTaskId();
		taskOutput.put("taskReferenceName", "task3");
		taskResult = setTaskResult(wfInstanceId, taskId, TaskResult.Status.COMPLETED, taskOutput);
		taskClient.updateTask(taskResult, "");

		Workflow workflow = workflowClient.getWorkflow(wfInstanceId, true);
		String taskReferenceName = workflow.getTaskByRefName("exclusiveJoin").getOutputData().get("taskReferenceName")
				.toString();

		assertEquals("task3", taskReferenceName);
		assertEquals(Workflow.WorkflowStatus.COMPLETED, workflow.getStatus());
	}

	@Test
	public void testDecision1FalseAndDecision3Default() throws Exception {
		workflowInput.put("decision_1", "false");
		workflowInput.put("decision_3", "null");

		StartWorkflowRequest startWorkflowRequest = new StartWorkflowRequest().withName(CONDUCTOR_WORKFLOW_DEF_NAME)
				.withCorrelationId("").withInput(workflowInput).withVersion(1);
		String wfInstanceId = workflowClient.startWorkflow(startWorkflowRequest);

		String taskId = taskClient.getPendingTaskForWorkflow(wfInstanceId, "task1").getTaskId();
		taskOutput.put("taskReferenceName", "task1");
		TaskResult taskResult = setTaskResult(wfInstanceId, taskId, TaskResult.Status.COMPLETED, taskOutput);
		taskClient.updateTask(taskResult, "");

		taskId = taskClient.getPendingTaskForWorkflow(wfInstanceId, "task4").getTaskId();
		taskOutput.put("taskReferenceName", "task4");
		taskResult = setTaskResult(wfInstanceId, taskId, TaskResult.Status.COMPLETED, taskOutput);
		taskClient.updateTask(taskResult, "");

		Workflow workflow = workflowClient.getWorkflow(wfInstanceId, true);
		String taskReferenceName = workflow.getTaskByRefName("exclusiveJoin").getOutputData().get("taskReferenceName")
				.toString();

		assertEquals("task4", taskReferenceName);
		assertEquals(Workflow.WorkflowStatus.COMPLETED, workflow.getStatus());
	}

	@Test
	public void testDecision1FalseAndDecision3True() throws Exception {
		workflowInput.put("decision_1", "false");
		workflowInput.put("decision_3", "true");

		StartWorkflowRequest startWorkflowRequest = new StartWorkflowRequest().withName(CONDUCTOR_WORKFLOW_DEF_NAME)
				.withCorrelationId("").withInput(workflowInput).withVersion(1);
		String wfInstanceId = workflowClient.startWorkflow(startWorkflowRequest);

		String taskId = taskClient.getPendingTaskForWorkflow(wfInstanceId, "task1").getTaskId();
		taskOutput.put("taskReferenceName", "task1");
		TaskResult taskResult = setTaskResult(wfInstanceId, taskId, TaskResult.Status.COMPLETED, taskOutput);
		taskClient.updateTask(taskResult, "");

		taskId = taskClient.getPendingTaskForWorkflow(wfInstanceId, "task4").getTaskId();
		taskOutput.put("taskReferenceName", "task4");
		taskResult = setTaskResult(wfInstanceId, taskId, TaskResult.Status.COMPLETED, taskOutput);
		taskClient.updateTask(taskResult, "");

		taskId = taskClient.getPendingTaskForWorkflow(wfInstanceId, "task5").getTaskId();
		taskOutput.put("taskReferenceName", "task5");
		taskResult = setTaskResult(wfInstanceId, taskId, TaskResult.Status.COMPLETED, taskOutput);
		taskClient.updateTask(taskResult, "");

		Workflow workflow = workflowClient.getWorkflow(wfInstanceId, true);
		String taskReferenceName = workflow.getTaskByRefName("exclusiveJoin").getOutputData().get("taskReferenceName")
				.toString();

		assertEquals("task5", taskReferenceName);
		assertEquals(Workflow.WorkflowStatus.COMPLETED, workflow.getStatus());
	}

	private TaskResult setTaskResult(String workflowInstanceId, String taskId, TaskResult.Status status,
			Map<String, Object> output) {
		TaskResult taskResult = new TaskResult();
		taskResult.setTaskId(taskId);
		taskResult.setWorkflowInstanceId(workflowInstanceId);
		taskResult.setStatus(status);
		taskResult.setOutputData(output);
		return taskResult;
	}

	private void registerWorkflowDefinitions() throws Exception {
		TaskWrapper taskWrapper = JsonUtils.fromJson("integration/scenarios/legacy/ExclusiveJoinTaskDef.json", TaskWrapper.class);
		metadataClient.registerTaskDefs(taskWrapper.getTaskDefs());

		WorkflowDef conductorWorkflowDef = JsonUtils.fromJson("integration/scenarios/legacy/ExclusiveJoinWorkflowDef.json",
				WorkflowDef.class);
		metadataClient.registerWorkflowDef(conductorWorkflowDef);
	}

	private void unRegisterWorkflowDefinitions() throws Exception {
		WorkflowDef conductorWorkflowDef = JsonUtils.fromJson("integration/scenarios/legacy/ExclusiveJoinWorkflowDef.json",
				WorkflowDef.class);
		metadataClient.unregisterWorkflowDef(conductorWorkflowDef.getName(), conductorWorkflowDef.getVersion());
	}

	@After
	public void unRegisterWorkflows() throws Exception {
		unRegisterWorkflowDefinitions();
	}

	@AfterClass
	public static void teardown() throws Exception {
		TestEnvironment.teardown();
		search.stop();
	}
}