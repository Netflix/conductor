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
package com.netflix.conductor.dao.queue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.dyno.queues.ShardSupplier;
import com.netflix.dyno.queues.redis.RedisDynoQueue;

import redis.clients.jedis.JedisCommands;

@Singleton
public class RedisQueueDAO implements QueueDAO {

	private static Logger logger = LoggerFactory.getLogger(RedisQueueDAO.class);

	private RedisQueues queues;

	private JedisCommands dynoClient;
	
	private JedisCommands dynoClientRead;

	private ShardSupplier ss;

	private String domain;

	private Configuration config;

	public RedisQueueDAO(JedisCommands dynoClient, JedisCommands dynoClientRead, ShardSupplier ss, Configuration config) {
		this.dynoClient = dynoClient;
		this.dynoClientRead = dynoClient;
		this.ss = ss;
		this.config = config;
		init();
	}

	public void init() {

		String rootNamespace = config.getProperty("workflow.namespace.queue.prefix", null);
		String stack = config.getStack();
		String prefix = rootNamespace + "." + stack;
		if (domain != null) {
			prefix = prefix + "." + domain;
		}
		queues = new RedisQueues(dynoClient, dynoClientRead, prefix, 60_000, 60_000);
		logger.info("DynoQueueDAO initialized with prefix " + prefix + "!");
	}

	@Override
	public void push(String queueName, String id, long offsetTimeInSecond) {
		Message msg = new Message(id, null);
		msg.setTimeout(offsetTimeInSecond, TimeUnit.SECONDS);
		queues.get(queueName).push(Arrays.asList(msg));
	}

	@Override
	public void push(String queueName, List<com.netflix.conductor.core.events.queue.Message> messages) {
		List<Message> msgs = messages.stream().map(msg -> new Message(msg.getId(), msg.getPayload())).collect(Collectors.toList());
		queues.get(queueName).push(msgs);
	}
	
	@Override
	public boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond) {
		Queue queue = queues.get(queueName);
		if (queue.get(id) != null) {
			return false;
		}
		Message msg = new Message(id, null);
		msg.setTimeout(offsetTimeInSecond, TimeUnit.SECONDS);
		queue.push(Arrays.asList(msg));
		return true;
	}

	@Override
	public List<String> pop(String queueName, int count, int timeout) {
		List<Message> msg = queues.get(queueName).pop(count, timeout, TimeUnit.MILLISECONDS);
		return msg.stream().map(m -> m.getId()).collect(Collectors.toList());
	}

	@Override
	public List<com.netflix.conductor.core.events.queue.Message> pollMessages(String queueName, int count, int timeout) {
		List<Message> msgs = queues.get(queueName).pop(count, timeout, TimeUnit.MILLISECONDS);
		return msgs.stream().map(msg -> new com.netflix.conductor.core.events.queue.Message(msg.getId(), msg.getPayload(), null)).collect(Collectors.toList());
	}
	
	@Override
	public void remove(String queueName, String messageId) {
		queues.get(queueName).remove(messageId);
	}

	@Override
	public int getSize(String queueName) {
		return (int) queues.get(queueName).size();
	}

	@Override
	public boolean ack(String queueName, String messageId) {
		return queues.get(queueName).ack(messageId);

	}
	
	@Override
	public boolean setUnackTimeout(String queueName, String messageId, long timeout) {
		return queues.get(queueName).setUnackTimeout(messageId, timeout);
	}

	@Override
	public void flush(String queueName) {
		Queue queue = queues.get(queueName);
		if (queue != null) {
			queue.clear();
		}
	}

	@Override
	public Map<String, Long> queuesDetail() {
		Map<String, Long> map = queues.queues().stream().collect(Collectors.toMap(queue -> queue.getName(), q -> q.size()));
		return map;
	}

	@Override
	public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
		Map<String, Map<String, Map<String, Long>>> map = queues.queues().stream()
				.collect(Collectors.toMap(queue -> queue.getName(), q -> q.shardSizes()));
		return map;
	}
	
	public void processUnacks(String queueName) {
		((RedisDynoQueue)queues.get(queueName)).processUnacks();;
	}

	@Override
	public boolean setOffsetTime(String queueName, String id, long offsetTimeInSecond) {
		Queue queue = queues.get(queueName);
		return queue.setTimeout(id, offsetTimeInSecond);
		
	}

}