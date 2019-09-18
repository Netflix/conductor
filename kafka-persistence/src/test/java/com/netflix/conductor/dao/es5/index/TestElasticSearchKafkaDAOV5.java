package com.netflix.conductor.dao.es5.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.KafkaConsumerDAO;
import com.netflix.conductor.dao.KafkaProducerDAO;
import com.netflix.conductor.dao.kafka.index.KafkaConsumer;
import com.netflix.conductor.dao.kafka.index.KafkaProducer;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.ElasticSearchTransportClientProvider;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearch;
import com.netflix.conductor.elasticsearch.SystemPropertiesElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.es5.EmbeddedElasticSearchV5;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestElasticSearchKafkaDAOV5 {

	private static ElasticSearchConfiguration configuration;
	private static Client elasticSearchClient;
	private static IndexDAO indexDAO;
	private static KafkaProducerDAO kafkaProducerDAO;
	private static EmbeddedElasticSearch embeddedElasticSearch;
	private static KafkaEmbedded embeddedKafka;
	private static KafkaConsumerDAO kafkaConsumerDAO;

	private Workflow workflow;

	@BeforeClass
	public static void startServer() throws Exception {
		System.setProperty(ElasticSearchConfiguration.EMBEDDED_PORT_PROPERTY_NAME, "9203");
		System.setProperty(ElasticSearchConfiguration.ELASTIC_SEARCH_URL_PROPERTY_NAME, "localhost:9303");
		System.setProperty(ElasticSearchConfiguration.KAFKA_INDEX_ENABLE, "true");

		configuration = new SystemPropertiesElasticSearchConfiguration();
		String host = configuration.getEmbeddedHost();
		int port = configuration.getEmbeddedPort();
		String clusterName = configuration.getEmbeddedClusterName();

		embeddedElasticSearch = new EmbeddedElasticSearchV5(clusterName, host, port);
		embeddedElasticSearch.start();

		ElasticSearchTransportClientProvider transportClientProvider =
				new ElasticSearchTransportClientProvider(configuration);
		elasticSearchClient = transportClientProvider.get();

		elasticSearchClient.admin()
				.cluster()
				.prepareHealth()
				.setWaitForGreenStatus()
				.execute()
				.get();

		ObjectMapper objectMapper = new ObjectMapper();
		embeddedKafka = new KafkaEmbedded(1, true, 1, "mytest");
		kafkaProducerDAO = new KafkaProducer(configuration);
		indexDAO = new ElasticSearchKafkaDAOV5(elasticSearchClient, configuration, objectMapper, kafkaProducerDAO);
		kafkaConsumerDAO = new KafkaConsumer(configuration, indexDAO);
	}

	@AfterClass
	public static void closeClient() throws Exception {
		if (elasticSearchClient != null) {
			elasticSearchClient.close();
		}

		embeddedElasticSearch.stop();
		kafkaConsumerDAO.close();
		kafkaProducerDAO.close();
	}

	@Before
	public void createTestWorkflow() throws Exception {
		// define indices
		indexDAO.setup();

		// initialize workflow
		workflow = new Workflow();
		workflow.getInput().put("requestId", "request id 001");
		workflow.getInput().put("hasAwards", true);
		workflow.getInput().put("channelMapping", 5);
		Map<String, Object> name = new HashMap<>();
		name.put("name", "The Who");
		name.put("year", 1970);
		Map<String, Object> name2 = new HashMap<>();
		name2.put("name", "The Doors");
		name2.put("year", 1975);

		List<Object> names = new LinkedList<>();
		names.add(name);
		names.add(name2);

		workflow.getOutput().put("name", name);
		workflow.getOutput().put("names", names);
		workflow.getOutput().put("awards", 200);

		Task task = new Task();
		task.setReferenceTaskName("task2");
		task.getOutputData().put("location", "http://location");
		task.setStatus(Status.COMPLETED);

		Task task2 = new Task();
		task2.setReferenceTaskName("task3");
		task2.getOutputData().put("refId", "abcddef_1234_7890_aaffcc");
		task2.setStatus(Status.SCHEDULED);

		workflow.getTasks().add(task);
		workflow.getTasks().add(task2);
	}

	@After
	public void tearDown() {
		deleteAllIndices();
	}

	private void deleteAllIndices() {

		ImmutableOpenMap<String, IndexMetaData> indices = elasticSearchClient.admin().cluster()
				.prepareState().get().getState()
				.getMetaData().getIndices();

		indices.forEach(cursor -> {
			try {
				elasticSearchClient.admin()
						.indices()
						.delete(new DeleteIndexRequest(cursor.value.getIndex().getName()))
						.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			}
		});
	}

	private boolean indexExists(final String index) {
		IndicesExistsRequest request = new IndicesExistsRequest(index);
		try {
			return elasticSearchClient.admin().indices().exists(request).get().isExists();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean doesMappingExist(final String index, final String mappingName) {
		GetMappingsRequest request = new GetMappingsRequest()
				.indices(index);
		try {
			GetMappingsResponse response = elasticSearchClient.admin()
					.indices()
					.getMappings(request)
					.get();

			return response.getMappings()
					.get(index)
					.containsKey(mappingName);
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void assertInitialSetup() throws Exception {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMww");
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		String taskLogIndex = "task_log_" + dateFormat.format(new Date());

		assertTrue("Index 'conductor' should exist", indexExists("conductor"));
		assertTrue("Index '" + taskLogIndex + "' should exist", indexExists(taskLogIndex));

		assertTrue("Mapping 'workflow' for index 'conductor' should exist", doesMappingExist("conductor", "workflow"));
		assertTrue("Mapping 'task' for index 'conductor' should exist", doesMappingExist("conductor", "task"));
	}

	@Test
	public void testWorkflowCRUD() throws Exception {
		String testWorkflowType = "test1workflow";
		String testId = "5";

		workflow.setWorkflowId(testId);
		workflow.setWorkflowType(testWorkflowType);

		// Create
		String workflowType = indexDAO.get(testId, "workflowType");
		assertNull("Workflow should not exist", workflowType);

		// Get
		indexDAO.indexWorkflow(workflow);

		await()
				.atMost(5, TimeUnit.SECONDS)
				.untilAsserted(
						() -> {
							String workflowtype = indexDAO.get(testId, "workflowType");
							assertEquals("Should have found our workflow type", testWorkflowType, workflowtype);
						}
				);

		// Update
		String newWorkflowType = "newworkflowtype";
		String[] keyChanges = {"workflowType"};
		String[] valueChanges = {newWorkflowType};

		indexDAO.updateWorkflow(testId, keyChanges, valueChanges);

		await()
				.atMost(3, TimeUnit.SECONDS)
				.untilAsserted(
						() -> {
							String actualWorkflowType = indexDAO.get(testId, "workflowType");
							assertEquals("Should have updated our new workflow type", newWorkflowType, actualWorkflowType);
						}
				);

		// Delete
		indexDAO.removeWorkflow(testId);

		workflowType = indexDAO.get(testId, "workflowType");
		assertNull("We should no longer have our workflow in the system", workflowType);
	}

	@Test
	public void testWorkflowSearch() {
		String workflowId = "search-workflow-id";
		workflow.setWorkflowId(workflowId);
		indexDAO.indexWorkflow(workflow);
        await()
                .atMost(3, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
							List<String> searchIds = indexDAO.searchWorkflows("", "workflowId:\"" + workflowId + "\"", 0, 100, Collections.singletonList("workflowId:ASC")).getResults();
							assertEquals(1, searchIds.size());
							assertEquals(workflowId, searchIds.get(0));
                        }
                );
	}

	@Test
	public void testSearchRecentRunningWorkflows() {
		workflow.setWorkflowId("completed-workflow");
		workflow.setStatus(Workflow.WorkflowStatus.COMPLETED);
		indexDAO.indexWorkflow(workflow);

		String workflowId = "recent-running-workflow-id";
		workflow.setWorkflowId(workflowId);
		workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
		workflow.setCreateTime(new Date().getTime());
		workflow.setUpdateTime(new Date().getTime());
		workflow.setEndTime(new Date().getTime());
		indexDAO.indexWorkflow(workflow);

		await()
				.atMost(3, TimeUnit.SECONDS)
				.untilAsserted(
						() -> {
							List<String> searchIds = indexDAO.searchRecentRunningWorkflows(1,0);
							assertEquals(1, searchIds.size());
							assertEquals(workflowId, searchIds.get(0));
						}
				);
	}

	@Test
	public void testSearchArchivableWorkflows() {
		String workflowId = "search-workflow-id";

		workflow.setWorkflowId(workflowId);
		workflow.setStatus(Workflow.WorkflowStatus.COMPLETED);
		workflow.setCreateTime(new Date().getTime());
		workflow.setUpdateTime(new Date().getTime());
		workflow.setEndTime(new Date().getTime());

		indexDAO.indexWorkflow(workflow);

		await()
				.atMost(3, TimeUnit.SECONDS)
				.untilAsserted(
						() -> {
							List<String> searchIds = indexDAO.searchArchivableWorkflows("conductor",10);
							assertEquals(1, searchIds.size());
							assertEquals(workflowId, searchIds.get(0));
						}
				);
	}

	@Test
	public void indexTask() throws Exception {
		String correlationId = "some-correlation-id";

		Task task = new Task();
		task.setTaskId("some-task-id");
		task.setWorkflowInstanceId("some-workflow-instance-id");
		task.setTaskType("some-task-type");
		task.setStatus(Status.FAILED);
		task.setInputData(new HashMap<String, Object>() {{ put("input_key", "input_value"); }});
		task.setCorrelationId(correlationId);
		task.setTaskDefName("some-task-def-name");
		task.setReasonForIncompletion("some-failure-reason");

		indexDAO.indexTask(task);

		await()
				.atMost(5, TimeUnit.SECONDS)
				.untilAsserted(() -> {
					SearchResult<String> result = indexDAO
							.searchTasks("correlationId='" + correlationId + "'", "*", 0, 10000, null);

					assertTrue("should return 1 or more search results", result.getResults().size() > 0);
					assertEquals("taskId should match the indexed task", "some-task-id", result.getResults().get(0));
				});
	}

}
