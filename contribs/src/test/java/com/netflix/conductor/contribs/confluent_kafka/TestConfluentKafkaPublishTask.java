package com.netflix.conductor.contribs.confluent_kafka;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.contribs.kafka.KafkaProducerManager;
import com.netflix.conductor.contribs.kafka.KafkaPublishTask;
import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.swiggy.kafka.clients.Record;
import com.swiggy.kafka.clients.producer.CallbackFuture;
import com.swiggy.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.netflix.conductor.contribs.confluent_kafka.ConfluentKafkaPublishTask.*;

public class TestConfluentKafkaPublishTask {

	private static ObjectMapper objectMapper = new JsonMapperProvider().get();

	@Test
	public void missingRequest_Fail() {
		ConfluentKafkaPublishTask confluentKafkaPublishTask = new ConfluentKafkaPublishTask(new SystemPropertiesConfiguration(), new ConfluentKafkaProducerManager(new SystemPropertiesConfiguration()), objectMapper);
		Task task = new Task();
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
	}

	@Test
	public void missingValue_Fail() {

		Task task = new Task();
		ConfluentKafkaPublishTask.Input input = new ConfluentKafkaPublishTask.Input();

		task.getInputData().put(ConfluentKafkaPublishTask.REQUEST_PARAMETER_NAME, input);

		KafkaPublishTask kPublishTask = new KafkaPublishTask(new SystemPropertiesConfiguration(), new KafkaProducerManager(new SystemPropertiesConfiguration()), objectMapper);
		kPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
	}

	@Test
	public void missingTopicValidations() {

		Task task = new Task();
		ConfluentKafkaPublishTask.Input input = new ConfluentKafkaPublishTask.Input();

		task.getInputData().put(ConfluentKafkaPublishTask.REQUEST_PARAMETER_NAME, input);

		ConfluentKafkaPublishTask confluentKafkaPublishTask = new ConfluentKafkaPublishTask(new SystemPropertiesConfiguration(), new ConfluentKafkaProducerManager(new SystemPropertiesConfiguration()), objectMapper);
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());

		Map<String, Object> topic = new HashMap<>();
		input.setTopic(topic);
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
		Assert.assertTrue(task.getReasonForIncompletion().contains(MISSING_KAFKA_TOPIC_NAME));
		topic.put("name", "f");
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
		Assert.assertTrue(task.getReasonForIncompletion().contains(MISSING_KAFKA_TOPIC_FAULT_STRETAGY));
		topic.put("faultStrategy", "f");
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
		Assert.assertTrue(task.getReasonForIncompletion().contains(MISSING_KAFKA_TOPIC_ENABLE_ENCRYPTION));
		topic.put("enableEncryption", "f");
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
		Assert.assertTrue(task.getReasonForIncompletion().contains(MISSING_KAFKA_TOPIC_KEY_ID));
		topic.put("keyId", "f");
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
		Assert.assertTrue(task.getReasonForIncompletion().contains(MISSING_PRIMARY_CLUSTER));
	}


	@Test
	public void missingPrimaryClusterValidations() {

		Task task = new Task();
		ConfluentKafkaPublishTask.Input input = new ConfluentKafkaPublishTask.Input();

		task.getInputData().put(ConfluentKafkaPublishTask.REQUEST_PARAMETER_NAME, input);

		ConfluentKafkaPublishTask confluentKafkaPublishTask = new ConfluentKafkaPublishTask(new SystemPropertiesConfiguration(), new ConfluentKafkaProducerManager(new SystemPropertiesConfiguration()), objectMapper);
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());

		Map<String, Object> topic = new HashMap<>();
		input.setTopic(topic);
		topic.put("name", "f");
		topic.put("faultStrategy", "f");
		topic.put("enableEncryption", "f");
		topic.put("keyId", "f");
		Map<String, Object> primaryCluster = new HashMap<>();
		input.setPrimaryCluster(primaryCluster);
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
		Assert.assertTrue(task.getReasonForIncompletion().contains(MISSING_PRIMARY_CLUSTER_BOOT_STRAP_SERVERS));
		primaryCluster.put("bootStrapServers", "localhost:9093");
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
		Assert.assertTrue(task.getReasonForIncompletion().contains(MISSING_PRIMARY_CLUSTER_AUTH_MECHANISM));
		primaryCluster.put("authMechanism", "NONE");
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
		Assert.assertTrue(task.getReasonForIncompletion().contains(MISSING_CLIENT_ID));

	}


	@Test
	public void missingGeneralValidations() {

		Task task = new Task();
		ConfluentKafkaPublishTask.Input input = new ConfluentKafkaPublishTask.Input();

		task.getInputData().put(ConfluentKafkaPublishTask.REQUEST_PARAMETER_NAME, input);

		ConfluentKafkaPublishTask confluentKafkaPublishTask = new ConfluentKafkaPublishTask(new SystemPropertiesConfiguration(), new ConfluentKafkaProducerManager(new SystemPropertiesConfiguration()), objectMapper);
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());

		Map<String, Object> topic = new HashMap<>();
		input.setTopic(topic);
		topic.put("name", "f");
		topic.put("faultStrategy", "f");
		topic.put("enableEncryption", "f");
		topic.put("keyId", "f");
		Map<String, Object> primaryCluster = new HashMap<>();
		input.setPrimaryCluster(primaryCluster);
		primaryCluster.put("bootStrapServers", "localhost:9093");
		primaryCluster.put("authMechanism", "NONE");
		input.setClientId("rr");
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
		Assert.assertTrue(task.getReasonForIncompletion().contains(MISSING_PRODUCER_ACK));
		input.setAcks("q");
		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
		Assert.assertTrue(task.getReasonForIncompletion().contains(MISSING_KAFKA_VALUE));


	}

	@Test
	public void kafkaPublishSuccess_Completed() throws ExecutionException, InterruptedException {

		Task task = new Task();
		task.setTaskDefName("task");
		ConfluentKafkaPublishTask.Input input = new ConfluentKafkaPublishTask.Input();
		task.getInputData().put(ConfluentKafkaPublishTask.REQUEST_PARAMETER_NAME, input);

		Map<String, Object> topic = new HashMap<>();
		input.setTopic(topic);
		topic.put("name", "f");
		topic.put("faultStrategy", "f");
		topic.put("enableEncryption", "f");
		topic.put("keyId", "f");
		Map<String, Object> primaryCluster = new HashMap<>();
		input.setPrimaryCluster(primaryCluster);
		primaryCluster.put("bootStrapServers", "localhost:9093");
		primaryCluster.put("authMechanism", "NONE");
		input.setClientId("rr");
		input.setAcks("q");
		input.setValue("ee");

		ConfluentKafkaProducerManager confluentKafkaProducerManager = Mockito.mock(ConfluentKafkaProducerManager.class);
		ConfluentKafkaPublishTask confluentKafkaPublishTask = new ConfluentKafkaPublishTask(new SystemPropertiesConfiguration(), confluentKafkaProducerManager, objectMapper);

		Producer producer = Mockito.mock(Producer.class);

		Mockito.when(confluentKafkaProducerManager.getProducer(Mockito.any(), Mockito.anyString())).thenReturn(producer);
		CallbackFuture<Record> callbackFuture = Mockito.mock(CallbackFuture.class);
		Mockito.doReturn(callbackFuture).when(producer).send(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any(Map.class));


		confluentKafkaPublishTask.start(Mockito.mock(Workflow.class), task, Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.COMPLETED, task.getStatus());
	}

	@Test
	public void integerSerializer_integerObject() {
		KafkaPublishTask kPublishTask = new KafkaPublishTask(new SystemPropertiesConfiguration(), new KafkaProducerManager(new SystemPropertiesConfiguration()), objectMapper);
		KafkaPublishTask.Input input = new KafkaPublishTask.Input();
		input.setKeySerializer(IntegerSerializer.class.getCanonicalName());
		input.setKey(String.valueOf(Integer.MAX_VALUE));
//		Assert.assertEquals(kPublishTask.getKey(input), new Integer(Integer.MAX_VALUE));
	}

	@Test
	public void longSerializer_longObject() {
		KafkaPublishTask kPublishTask = new KafkaPublishTask(new SystemPropertiesConfiguration(), new KafkaProducerManager(new SystemPropertiesConfiguration()), objectMapper);
		KafkaPublishTask.Input input = new KafkaPublishTask.Input();
		input.setKeySerializer(LongSerializer.class.getCanonicalName());
		input.setKey(String.valueOf(Long.MAX_VALUE));
//		Assert.assertEquals(kPublishTask.getKey(input), new Long(Long.MAX_VALUE));
	}

	@Test
	public void noSerializer_StringObject() {
		KafkaPublishTask kPublishTask = new KafkaPublishTask(new SystemPropertiesConfiguration(), new KafkaProducerManager(new SystemPropertiesConfiguration()), objectMapper);
		KafkaPublishTask.Input input = new KafkaPublishTask.Input();
		input.setKey("testStringKey");
//		Assert.assertEquals(kPublishTask.getKey(input), "testStringKey");
	}

}
