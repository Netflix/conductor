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
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.config.TestConfiguration;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.SemaphoreDAO;
import com.netflix.conductor.dao.dynomite.queue.DynoQueueDAO;
import com.netflix.conductor.dao.redis.JedisMock;
import com.netflix.conductor.dyno.DynoProxy;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.queues.ShardSupplier;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.commands.JedisCommands;

/**
 * 
 * @author Viren
 *
 */
public class RedisSemaphoreDAOTest {

	private static ObjectMapper objectMapper = new JsonMapperProvider().get();
	
	private SemaphoreDAO semaphoreDAO;

	@Before
	public void init() {
		JedisCommands jedisMock = new JedisMock();
        DynoProxy dynoClient = new DynoProxy(jedisMock);
        Configuration config = new TestConfiguration();
		semaphoreDAO = new RedisSemaphoreDAO(dynoClient, objectMapper, config);
	}

	@Rule
	public ExpectedException expected = ExpectedException.none();

	@Test
	public void test() {
		String semaphore_name = "test_semaphore";
		double timeout = 10000.0;
		int limit = 5;
		for (int i = 1; i < 6; i++) {
			assertTrue(
				String.format("Initial try acquire %d/5", i),
				semaphoreDAO.tryAcquire(semaphore_name, String.valueOf(i), limit, timeout)
			);
		}
		assertFalse(
			"Try to acquire over the allowed limit", 
			semaphoreDAO.tryAcquire(semaphore_name, "identifier", limit, timeout)
		);
		assertTrue(
			"Release No3", 
			semaphoreDAO.release(semaphore_name, "3")
		);
		assertTrue(
			"Acquire No6",
			semaphoreDAO.tryAcquire(semaphore_name, "6", limit, timeout)
		);
		for (int i = 1; i < 7; i++) {
			assertTrue(
				String.format("Final release %d/6", i),
				semaphoreDAO.release(semaphore_name, String.valueOf(i)) || i == 3 
			);
		}
	}
}
