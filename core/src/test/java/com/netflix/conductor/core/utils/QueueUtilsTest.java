package com.netflix.conductor.core.utils;

import com.netflix.conductor.core.execution.tasks.IsolatedTaskQueueProducer;
import org.junit.Assert;
import org.junit.Test;

public class QueueUtilsTest {

	@Test
	public void queueNameWithTypeAndIsolationGroup() {

		String queueNameGenerated = QueueUtils.getQueueName("tType", null, "isolationGroup");

		Assert.assertEquals("tType-isolationGroup", queueNameGenerated);
	}

	@Test
	public void queueNameWithTypeAndNoIsolationGroup() {

		String queueNameGenerated = QueueUtils.getQueueName("tType", null, null);

		Assert.assertEquals("tType", queueNameGenerated);
	}

	@Test
	public void notIsolatedIfSeparatorNotPresent() {

		Assert.assertFalse(QueueUtils.isIsolatedQueue("notIsolated"));

	}

	@Test
	public void testGetQueueDomain() {

		Assert.assertEquals(QueueUtils.getQueueDomain("domain:notIsolated"),"domain");

	}

	@Test
	public void testGetQueueDomainEmpty() {

		Assert.assertEquals(QueueUtils.getQueueDomain("notIsolated"),"");

	}

	@Test
	public void testGetQueueDomainWithIsolationGroup() {

		Assert.assertEquals(QueueUtils.getQueueDomain("domain:notIsolated-isolated"),"domain");

	}

	@Test
	public void testGetQueueName() {

		Assert.assertEquals("domain:taskType-isolated",QueueUtils.getQueueName("taskType","domain","isolated"));

	}

	@Test
	public void testGetTaskType() {

		Assert.assertEquals("taskType",QueueUtils.getTaskType("domain:taskType-isolated"));

	}

	@Test
	public void testGetTaskTypeWithoutDomain() {

		Assert.assertEquals("taskType",QueueUtils.getTaskType("taskType-isolated"));

	}

	@Test
	public void testGetTaskTypeWithoutDomainAndWithoutIsolationGroup() {

		Assert.assertEquals("taskType",QueueUtils.getTaskType("taskType"));

	}

}
