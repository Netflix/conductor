/*
 * Copyright 2019 Netflix, Inc.
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
package com.netflix.conductor.dao.dynomite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.events.EventHandler.Action;
import com.netflix.conductor.common.metadata.events.EventHandler.Action.Type;
import com.netflix.conductor.common.metadata.events.EventHandler.StartWorkflow;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskDef.RetryLogic;
import com.netflix.conductor.common.metadata.tasks.TaskDef.TimeoutPolicy;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.config.TestConfiguration;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.redis.JedisMock;
import com.netflix.conductor.dyno.DynoProxy;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.commands.JedisCommands;

/**
 * @author Viren
 *
 */
public class RedisMetadataDAOTest {

	private RedisMetadataDAO redisMetadataDAO;
	
	private static ObjectMapper objectMapper = new JsonMapperProvider().get();

	@Before
	public void init() {
		Configuration config = new TestConfiguration();
		JedisCommands jedisMock = new JedisMock();
		DynoProxy dynoClient = new DynoProxy(jedisMock);
		
		redisMetadataDAO = new RedisMetadataDAO(dynoClient, objectMapper, config);
	}

    @Test(expected = ApplicationException.class)
	public void testDup() {
		WorkflowDef def = new WorkflowDef();
		def.setName("testDup");
		def.setVersion(1);
		
		redisMetadataDAO.create(def);
		redisMetadataDAO.create(def);
	}
	
	@Test
	public void testWorkflowDefOperations() {
		
		WorkflowDef def = new WorkflowDef();
		def.setName("test");
		def.setVersion(1);
		def.setDescription("description");
		def.setCreatedBy("unit_test");
		def.setCreateTime(1L);
		def.setOwnerApp("ownerApp");
		def.setUpdatedBy("unit_test2");
		def.setUpdateTime(2L);
		
		redisMetadataDAO.create(def);
		
		List<WorkflowDef> all = redisMetadataDAO.getAll();
		assertNotNull(all);
		assertEquals(1, all.size());
		assertEquals("test", all.get(0).getName());
		assertEquals(1, all.get(0).getVersion());
		
		WorkflowDef found = redisMetadataDAO.get("test", 1).get();
		assertTrue(EqualsBuilder.reflectionEquals(def, found));
		
		def.setVersion(2);
		redisMetadataDAO.create(def);
		
		all = redisMetadataDAO.getAll();
		assertNotNull(all);
		assertEquals(2, all.size());
		assertEquals("test", all.get(0).getName());
		assertEquals(1, all.get(0).getVersion());
		
		found = redisMetadataDAO.getLatest(def.getName()).get();
		assertEquals(def.getName(), found.getName());
		assertEquals(def.getVersion(), found.getVersion());
		assertEquals(2, found.getVersion());

		all = redisMetadataDAO.getAllVersions(def.getName());
		assertNotNull(all);
		assertEquals(2, all.size());
		assertEquals("test", all.get(0).getName());
		assertEquals("test", all.get(1).getName());
		assertEquals(1, all.get(0).getVersion());
		assertEquals(2, all.get(1).getVersion());
		
		def.setDescription("updated");
		redisMetadataDAO.update(def);
		found = redisMetadataDAO.get(def.getName(), def.getVersion()).get();
		assertEquals(def.getDescription(), found.getDescription());
		
		List<String> allnames = redisMetadataDAO.findAll();
		assertNotNull(allnames);
		assertEquals(1, allnames.size());
		assertEquals(def.getName(), allnames.get(0));

		redisMetadataDAO.removeWorkflowDef("test", 1);
		Optional<WorkflowDef> deleted = redisMetadataDAO.get("test", 1);
		assertFalse(deleted.isPresent());
		redisMetadataDAO.removeWorkflowDef("test", 2);
		Optional<WorkflowDef> latestDef = redisMetadataDAO.getLatest("test");
		assertFalse(latestDef.isPresent());

		WorkflowDef[] workflowDefsArray = new WorkflowDef[3];
		for(int i=1; i <=3; i++) {
			workflowDefsArray[i-1] = new WorkflowDef();
			workflowDefsArray[i-1].setName("test");
			workflowDefsArray[i-1].setVersion(i);
			workflowDefsArray[i-1].setDescription("description");
			workflowDefsArray[i-1].setCreatedBy("unit_test");
			workflowDefsArray[i-1].setCreateTime(1L);
			workflowDefsArray[i-1].setOwnerApp("ownerApp");
			workflowDefsArray[i-1].setUpdatedBy("unit_test2");
			workflowDefsArray[i-1].setUpdateTime(2L);
			redisMetadataDAO.create( workflowDefsArray[i-1]);
		}
		redisMetadataDAO.removeWorkflowDef("test", 1);
		redisMetadataDAO.removeWorkflowDef("test", 2);
		WorkflowDef workflow = redisMetadataDAO.getLatest("test").get();
		assertEquals(workflow.getVersion(), 3);
	}

	@Test(expected = ApplicationException.class)
	public void removeInvalidWorkflowDef() throws Exception {
		WorkflowDef def = new WorkflowDef();
		redisMetadataDAO.removeWorkflowDef("hello", 1);
	}
	
	@Test
	public void testTaskDefOperations() throws Exception {
		
		TaskDef def = new TaskDef("taskA");
		def.setDescription("description");
		def.setCreatedBy("unit_test");
		def.setCreateTime(1L);
		def.setInputKeys(Arrays.asList("a","b","c"));
		def.setOutputKeys(Arrays.asList("01","o2"));
		def.setOwnerApp("ownerApp");
		def.setRetryCount(3);
		def.setRetryDelaySeconds(100);
		def.setRetryLogic(RetryLogic.FIXED);
		def.setTimeoutPolicy(TimeoutPolicy.ALERT_ONLY);
		def.setUpdatedBy("unit_test2");
		def.setUpdateTime(2L);
		def.setRateLimitPerFrequency(50);
		def.setRateLimitFrequencyInSeconds(1);

		redisMetadataDAO.createTaskDef(def);
		
		TaskDef found = redisMetadataDAO.getTaskDef(def.getName());
		assertTrue(EqualsBuilder.reflectionEquals(def, found));
		
		def.setDescription("updated description");
		redisMetadataDAO.updateTaskDef(def);
		found = redisMetadataDAO.getTaskDef(def.getName());
		assertTrue(EqualsBuilder.reflectionEquals(def, found));
		assertEquals("updated description", found.getDescription());
		
		for(int i = 0; i < 9; i++) {
			TaskDef tdf = new TaskDef("taskA" + i);
			redisMetadataDAO.createTaskDef(tdf);
		}
		
		List<TaskDef> all = redisMetadataDAO.getAllTaskDefs();
		assertNotNull(all);
		assertEquals(10, all.size());
		Set<String> allnames = all.stream().map(TaskDef::getName).collect(Collectors.toSet());
		assertEquals(10, allnames.size());
		List<String> sorted = allnames.stream().sorted().collect(Collectors.toList());
		assertEquals(def.getName(), sorted.get(0));
		
		for(int i = 0; i < 9; i++) {
			assertEquals(def.getName() + i, sorted.get(i+1));
		}
		
		for(int i = 0; i < 9; i++) {
			redisMetadataDAO.removeTaskDef(def.getName() + i);
		}
		all = redisMetadataDAO.getAllTaskDefs();
		assertNotNull(all);
		assertEquals(1, all.size());
		assertEquals(def.getName(), all.get(0).getName());
	}

	@Test(expected = ApplicationException.class)
	public void testRemoveTaskDef() throws Exception {
		redisMetadataDAO.removeTaskDef("test" + UUID.randomUUID().toString());
	}
	
	@Test
	public void testEventHandlers() {
		String event1 = "SQS::arn:account090:sqstest1";
		String event2 = "SQS::arn:account090:sqstest2";
		
		EventHandler eh = new EventHandler();
		eh.setName(UUID.randomUUID().toString());
		eh.setActive(false);
		Action action = new Action();
		action.setAction(Type.start_workflow);
		action.setStart_workflow(new StartWorkflow());
		action.getStart_workflow().setName("workflow_x");
		eh.getActions().add(action);
		eh.setEvent(event1);
		
		redisMetadataDAO.addEventHandler(eh);
		List<EventHandler> all = redisMetadataDAO.getEventHandlers();
		assertNotNull(all);
		assertEquals(1, all.size());
		assertEquals(eh.getName(), all.get(0).getName());
		assertEquals(eh.getEvent(), all.get(0).getEvent());

		List<EventHandler> byEvents = redisMetadataDAO.getEventHandlersForEvent(event1, true);
		assertNotNull(byEvents);
		assertEquals(0, byEvents.size());		//event is marked as in-active
		
		eh.setActive(true);
		eh.setEvent(event2);
		redisMetadataDAO.updateEventHandler(eh);
		
		all = redisMetadataDAO.getEventHandlers();
		assertNotNull(all);
		assertEquals(1, all.size());

		byEvents = redisMetadataDAO.getEventHandlersForEvent(event1, true);
		assertNotNull(byEvents);
		assertEquals(0, byEvents.size());
		
		byEvents = redisMetadataDAO.getEventHandlersForEvent(event2, true);
		assertNotNull(byEvents);
		assertEquals(1, byEvents.size());
	}

}
