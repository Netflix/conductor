package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.service.MetadataService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

public class TestIsolatedTaskQueueProducer {

	@Test
	public void addTaskQueuesAddsElementToQueue() throws InterruptedException {

		SystemTaskWorkerCoordinator.taskNameWorkFlowTaskMapping.put("HTTP", Mockito.mock(WorkflowSystemTask.class));
		MetadataService metadataService = Mockito.mock(MetadataService.class);
		IsolatedTaskQueueProducer isolatedTaskQueueProducer = new IsolatedTaskQueueProducer(metadataService, Mockito.mock(Configuration.class));
		TaskDef taskDef = new TaskDef();
		taskDef.setIsolationGroupId("isolated");
		Mockito.when(metadataService.getTaskDefs()).thenReturn(Arrays.asList(taskDef));
		isolatedTaskQueueProducer.addTaskQueues();

		Assert.assertFalse(SystemTaskWorkerCoordinator.queue.isEmpty());

	}



	@Test
	public void interruptReturns() throws InterruptedException {

		MetadataService metadataService = Mockito.mock(MetadataService.class);
		IsolatedTaskQueueProducer isolatedTaskQueueProducer = new IsolatedTaskQueueProducer(metadataService, Mockito.mock(Configuration.class));
		Thread thread = new Thread(isolatedTaskQueueProducer::syncTaskQueues);
		thread.start();
		thread.interrupt();
		thread.join();
		
		Assert.assertFalse(thread.isAlive());

	}

}
