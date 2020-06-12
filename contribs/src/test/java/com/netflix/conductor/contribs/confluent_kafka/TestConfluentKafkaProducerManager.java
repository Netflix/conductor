package com.netflix.conductor.contribs.confluent_kafka;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import com.swiggy.kafka.clients.configs.AuthMechanism;
import com.swiggy.kafka.clients.configs.ProducerConfig;
import com.swiggy.kafka.clients.configs.Topic;
import com.swiggy.kafka.clients.configs.enums.ProducerAcks;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestConfluentKafkaProducerManager {

	@Test
	public void testRequestTimeoutSetFromDefault() {
		ConfluentKafkaProducerManager confluentKafkaProducerManager = new ConfluentKafkaProducerManager(new SystemPropertiesConfiguration());
		Assert.assertEquals( confluentKafkaProducerManager.requestTimeoutConfig, "100");
	}

	public ConfluentKafkaPublishTask.Input getInput(boolean ha) {
		ConfluentKafkaPublishTask.Input input = new ConfluentKafkaPublishTask.Input();

		Map<String, Object> topic = new HashMap<>();
		input.setTopic(topic);
		topic.put("name", "flo-event-logs");
		topic.put("faultStrategy", "NONE");
		topic.put("enableEncryption", "true");
		topic.put("keyId", "ff-fxm");
		Map<String, Object> primaryCluster = new HashMap<>();
		input.setPrimaryCluster(primaryCluster);
		primaryCluster.put("bootStrapServers", "localhost:9093");
		primaryCluster.put("authMechanism", "SASL_PLAIN");
		if (ha) {
			Map<String, Object> secondaryCluster = new HashMap<>();
			input.setSecondaryCluster(secondaryCluster);
			secondaryCluster.put("bootStrapServers", "localhost:9092");
			secondaryCluster.put("authMechanism", "SASL_PLAIN");
		}
		input.setClientId("rr");
		input.setAcks("q");
		input.setValue("ee");
		input.setAcks(ProducerAcks.ALL.name());
		return input;
	}

	public void validateProducerConfig(ProducerConfig producerConfig, boolean ha) {
		if (ha) {
			Assert.assertEquals(producerConfig.getPrimary().getPassword(), "primarysecret");
			Assert.assertEquals(producerConfig.getPrimary().getUsername(), "primarykey");
			Assert.assertEquals(producerConfig.getSecondary().getPassword(), "secondarysecret");
			Assert.assertEquals(producerConfig.getSecondary().getUsername(), "secondarykey");
		} else {
		Assert.assertEquals(producerConfig.getPrimary().getPassword(), "secret");
		Assert.assertEquals(producerConfig.getPrimary().getUsername(), "key");}
		Assert.assertEquals(producerConfig.getClientId(), "rr");
		Assert.assertEquals(producerConfig.isEnableCompression(), false);
		Assert.assertEquals(producerConfig.getPrimary().getBootstrapServers(), "localhost:9093");
		Assert.assertEquals(producerConfig.getPrimary().getAuthMechanism(), AuthMechanism.SASL_PLAIN);
		if (ha) {
			Assert.assertEquals(producerConfig.getSecondary().getBootstrapServers(), "localhost:9092");
			Assert.assertEquals(producerConfig.getSecondary().getAuthMechanism(), AuthMechanism.SASL_PLAIN);
		}
		Assert.assertEquals(producerConfig.getTopics().size(), 1);
		for (Map.Entry<String, Topic> entry : producerConfig.getTopics().entrySet()) {
			Assert.assertEquals(entry.getKey(), "flo-event-logs");
			Topic topic1 = entry.getValue();
			Assert.assertEquals(topic1.getName(), "flo-event-logs");
			Assert.assertEquals(topic1.getFaultStrategy(), Topic.FaultStrategy.NONE);
			Assert.assertEquals(topic1.isEnableEncryption(), true);
			Assert.assertEquals(topic1.getKeyId(), "ff-fxm");
		}
	}

	@Test
	public void testBatchConfigurations() {
		ConfluentKafkaProducerManager confluentKafkaProducerManager = new ConfluentKafkaProducerManager(new SystemPropertiesConfiguration());
		Task task = new Task();
		task.setTaskDefName("task");
		ConfluentKafkaPublishTask.Input input = getInput(false);
		input.setClusterType(ConfluentKafkaProducerManager.ClusterType.BATCH.name());
		System.setProperty("TASK_KAFKA_BATCH_PRIMARY_API_SECRET", "secret");
		System.setProperty("TASK_KAFKA_BATCH_PRIMARY_API_KEY", "key");
		ProducerConfig producerConfig = confluentKafkaProducerManager.getProducerProperties(input, task.getTaskDefName(), "test");
		validateProducerConfig(producerConfig, false);
	}

	@Test
	public void testWorkflowNameBatchConfigurations() {
		ConfluentKafkaProducerManager confluentKafkaProducerManager = new ConfluentKafkaProducerManager(new SystemPropertiesConfiguration());
		Task task = new Task();
		task.setTaskDefName("task");
		ConfluentKafkaPublishTask.Input input = getInput(false);
		input.setClusterType(ConfluentKafkaProducerManager.ClusterType.BATCH.name());
		System.setProperty("TEST_TASK_KAFKA_BATCH_PRIMARY_API_SECRET", "secret");
		System.setProperty("TEST_TASK_KAFKA_BATCH_PRIMARY_API_KEY", "key");
		ProducerConfig producerConfig = confluentKafkaProducerManager.getProducerProperties(input, task.getTaskDefName(), "test");
		validateProducerConfig(producerConfig, false);
	}

	@Test
	public void testTXNConfigurations() {
		ConfluentKafkaProducerManager confluentKafkaProducerManager = new ConfluentKafkaProducerManager(new SystemPropertiesConfiguration());
		Task task = new Task();
		task.setTaskDefName("task");
		ConfluentKafkaPublishTask.Input input = getInput(false);
		input.setClusterType(ConfluentKafkaProducerManager.ClusterType.TXN.name());
		System.setProperty("TASK_KAFKA_TXN_PRIMARY_API_SECRET", "secret");
		System.setProperty("TASK_KAFKA_TXN_PRIMARY_API_KEY", "key");
		ProducerConfig producerConfig = confluentKafkaProducerManager.getProducerProperties(input, task.getTaskDefName(), "test");
		validateProducerConfig(producerConfig, false);
	}

	@Test
	public void testWorkflowTXNConfigurations() {
		ConfluentKafkaProducerManager confluentKafkaProducerManager = new ConfluentKafkaProducerManager(new SystemPropertiesConfiguration());
		Task task = new Task();
		task.setTaskDefName("task");
		ConfluentKafkaPublishTask.Input input = getInput(false);
		input.setClusterType(ConfluentKafkaProducerManager.ClusterType.TXN.name());
		System.setProperty("TEST_TASK_KAFKA_TXN_PRIMARY_API_SECRET", "secret");
		System.setProperty("TEST_TASK_KAFKA_TXN_PRIMARY_API_KEY", "key");
		ProducerConfig producerConfig = confluentKafkaProducerManager.getProducerProperties(input, task.getTaskDefName(), "test");
		validateProducerConfig(producerConfig, false);
	}

	@Test
	public void testTXNHAConfigurations() {
		ConfluentKafkaProducerManager confluentKafkaProducerManager = new ConfluentKafkaProducerManager(new SystemPropertiesConfiguration());
		Task task = new Task();
		task.setTaskDefName("task");
		ConfluentKafkaPublishTask.Input input = getInput(true);
		input.setClusterType("TXN_HA");
		System.setProperty("TASK_KAFKA_TXN_HA_PRIMARY_API_SECRET", "primarysecret");
		System.setProperty("TASK_KAFKA_TXN_HA_PRIMARY_API_KEY", "primarykey");
		System.setProperty("TASK_KAFKA_TXN_HA_SECONDARY_API_SECRET", "secondarysecret");
		System.setProperty("TASK_KAFKA_TXN_HA_SECONDARY_API_KEY", "secondarykey");
		ProducerConfig producerConfig = confluentKafkaProducerManager.getProducerProperties(input, task.getTaskDefName(), "test");
		validateProducerConfig(producerConfig, true);
	}

	@Test
	public void testTXNHAWorkflowConfigurations() {
		ConfluentKafkaProducerManager confluentKafkaProducerManager = new ConfluentKafkaProducerManager(new SystemPropertiesConfiguration());
		Task task = new Task();
		task.setTaskDefName("task");
		ConfluentKafkaPublishTask.Input input = getInput(true);
		input.setClusterType("TXN_HA");
		System.setProperty("TEST_TASK_KAFKA_TXN_HA_PRIMARY_API_SECRET", "primarysecret");
		System.setProperty("TEST_TASK_KAFKA_TXN_HA_PRIMARY_API_KEY", "primarykey");
		System.setProperty("TEST_TASK_KAFKA_TXN_HA_SECONDARY_API_SECRET", "secondarysecret");
		System.setProperty("TEST_TASK_KAFKA_TXN_HA_SECONDARY_API_KEY", "secondarykey");
		ProducerConfig producerConfig = confluentKafkaProducerManager.getProducerProperties(input, task.getTaskDefName(), "test");
		validateProducerConfig(producerConfig, true);
	}

}
