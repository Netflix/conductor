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
package com.netflix.conductor.server;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.conductor.contribs.HttpModule;
import com.netflix.conductor.contribs.auth.AuthTask;
import com.netflix.conductor.contribs.json.JsonJqTransform;
import com.netflix.conductor.contribs.validation.ValidationTask;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.config.CoreModule;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.dynomite.DynoProxy;
import com.netflix.conductor.dao.dynomite.RedisExecutionDAO;
import com.netflix.conductor.dao.dynomite.RedisMetadataDAO;
import com.netflix.conductor.dao.dynomite.queue.DynoQueueDAO;
import com.netflix.conductor.dao.es5.ElasticSearchExecutionDAO;
import com.netflix.conductor.dao.es5.ElasticSearchMetadataDAO;
import com.netflix.conductor.dao.es5.ElasticSearchQueueDAO;
import com.netflix.conductor.dao.es5.index.ElasticSearchDAO;
import com.netflix.conductor.dao.es5.index.ElasticsearchModule;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.queues.redis.DynoShardSupplier;
import redis.clients.jedis.JedisCommands;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Viren
 *
 */
public class ServerModule extends AbstractModule {
	
	private int maxThreads = 50;
	
	private ExecutorService es;
	
	private JedisCommands dynoConn;
	
	private HostSupplier hs;
	
	private String region;
	
	private String localRack;
	
	private ConductorConfig config;
	
	private ConductorServer.DB db;

	public ServerModule(JedisCommands jedis, HostSupplier hs, ConductorConfig config, ConductorServer.DB db) {
		this.dynoConn = jedis;
		this.hs = hs;
		this.config = config;
		this.region = config.getRegion();
		this.localRack = config.getAvailabilityZone();
		this.db = db;
	}
	
	@Override
	protected void configure() {
		
		configureExecutorService();
		
		bind(Configuration.class).toInstance(config);

		install(new ElasticsearchModule());
		bind(IndexDAO.class).to(ElasticSearchDAO.class);

		if (ConductorServer.DB.elasticsearch.equals(db)) {
			bind(ExecutionDAO.class).to(ElasticSearchExecutionDAO.class);
			bind(MetadataDAO.class).to(ElasticSearchMetadataDAO.class);
			bind(QueueDAO.class).to(ElasticSearchQueueDAO.class);
		} else {
			String localDC = localRack.replaceAll(region, "");
			DynoShardSupplier ss = new DynoShardSupplier(hs, region, localDC);
			DynoQueueDAO queueDao = new DynoQueueDAO(dynoConn, dynoConn, ss, config);

			DynoProxy proxy = new DynoProxy(dynoConn);
			bind(DynoProxy.class).toInstance(proxy);

			bind(ExecutionDAO.class).to(RedisExecutionDAO.class);
			bind(MetadataDAO.class).to(RedisMetadataDAO.class);
			bind(DynoQueueDAO.class).toInstance(queueDao);
			bind(QueueDAO.class).to(DynoQueueDAO.class);
		}

		install(new CoreModule());
		install(new JerseyModule());
		install(new HttpModule());
		new JsonJqTransform();
		new ValidationTask();
		new AuthTask(config);
		List<AbstractModule> additionalModules = config.getAdditionalModules();
		if(additionalModules != null) {
			for(AbstractModule additionalModule : additionalModules) {
				install(additionalModule);
			}
		}
	}
	
	@Provides
	public ExecutorService getExecutorService(){
		return this.es;
	}
	
	private void configureExecutorService(){
		AtomicInteger count = new AtomicInteger(0);
		this.es = java.util.concurrent.Executors.newFixedThreadPool(maxThreads, new ThreadFactory() {
			
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName("conductor-worker-" + count.getAndIncrement());
				return t;
			}
		});
	}
}
