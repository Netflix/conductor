/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.matcher.Matchers;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.conductor.annotations.Service;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.config.CoreModule;
import com.netflix.conductor.core.execution.WorkflowSweeper;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dyno.DynoProxy;
import com.netflix.conductor.dao.dynomite.RedisExecutionDAO;
import com.netflix.conductor.dao.dynomite.RedisMetadataDAO;
import com.netflix.conductor.dao.dynomite.queue.DynoQueueDAO;
import com.netflix.conductor.dao.esrest.index.ElasticSearchDAO;
import com.netflix.conductor.dao.esrest.index.ElasticsearchModule;
import com.netflix.conductor.dao.mysql.MySQLWorkflowModule;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.queues.shard.SingleShardSupplier;

import redis.clients.jedis.JedisCommands;
import com.netflix.conductor.core.config.ValidationModule;
import com.netflix.conductor.core.execution.WorkflowSweeper;
import com.netflix.conductor.dyno.SystemPropertiesDynomiteConfiguration;
import com.netflix.conductor.grpc.server.GRPCModule;
import com.netflix.conductor.interceptors.ServiceInterceptor;
import com.netflix.conductor.jetty.server.JettyModule;
import com.netflix.runtime.health.guice.HealthModule;

import javax.validation.Validator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Viren
 */
public class ServerModule extends AbstractModule {

	private int maxThreads = 50;

	private ExecutorService es;

	private JedisCommands dynoConn;

	private HostSupplier hs;

	private String region;

	private String localRack;

	private Configuration config;

	private ConductorServer.DB db;

	public ServerModule() {
		// TODO Auto-generated constructor stub
	}

	public ServerModule(JedisCommands jedis, HostSupplier hs, Configuration config, ConductorServer.DB db) {
		this.dynoConn = jedis;
		this.hs = hs;
		this.config = config;
		this.region = config.getRegion();
		this.localRack = config.getAvailabilityZone();
		this.db = db;

	}


	// @Override have to confirm if we need this
	protected void configure2() {

		configureExecutorService();

		bind(Configuration.class).toInstance(config);

		if (db == ConductorServer.DB.mysql) {
			install(new MySQLWorkflowModule());
		} else {
			String localDC = localRack;
			localDC = localDC.replaceAll(region, "");
			SingleShardSupplier ss = new SingleShardSupplier("custom");
			DynoQueueDAO queueDao = new DynoQueueDAO(dynoConn, dynoConn, ss, config);

			bind(MetadataDAO.class).to(RedisMetadataDAO.class);
			bind(ExecutionDAO.class).to(RedisExecutionDAO.class);
			bind(DynoQueueDAO.class).toInstance(queueDao);
			bind(QueueDAO.class).to(DynoQueueDAO.class);

			DynoProxy proxy = new DynoProxy(dynoConn);
			bind(DynoProxy.class).toInstance(proxy);
			bind(WorkflowSweeper.class).asEagerSingleton();
		}
	}


    @Override
    protected void configure() {
        install(new CoreModule());
        install(new ValidationModule());
        install(new ArchaiusModule());
        install(new HealthModule());
        install(new JettyModule());
        install(new GRPCModule());

        bindInterceptor(Matchers.any(), Matchers.annotatedWith(Service.class), new ServiceInterceptor(getProvider(Validator.class)));
        bind(ObjectMapper.class).toProvider(JsonMapperProvider.class);
        bind(Configuration.class).to(SystemPropertiesDynomiteConfiguration.class);
        bind(ExecutorService.class).toProvider(ExecutorServiceProvider.class).in(Scopes.SINGLETON);
        bind(WorkflowSweeper.class).asEagerSingleton();
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
