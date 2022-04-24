/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.servlet.GuiceFilter;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.dao.es6rest.Elasticsearch6Embedded;
import com.netflix.conductor.redis.utils.JedisMock;
import com.netflix.conductor.service.MetricService;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.sun.jersey.api.client.Client;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

import javax.servlet.DispatcherType;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.*;

/**
 * @author Viren
 */
public class ConductorServer {

	private static Logger logger = LoggerFactory.getLogger(ConductorServer.class);

	enum DB {
		redis, dynomite, memory, elasticsearch, aurora
	}

	private enum SearchMode {
		elasticsearch, memory, none
	}

	private ServerModule sm;

	private Server server;

	private ConductorConfig cc;

	private DB db;

	private SearchMode mode;

	public ConductorServer(ConductorConfig cc) {
		this.cc = cc;
		String dynoClusterName = cc.getProperty("workflow.dynomite.cluster.name", "");

		List<Host> dynoHosts = new LinkedList<>();
		String dbstring = cc.getProperty("db", "memory");
		try {
			db = DB.valueOf(dbstring);
		} catch (IllegalArgumentException ie) {
			logger.error("Invalid db name: " + dbstring + ", supported values are: redis, dynomite, memory");
			System.exit(1);
		}

		String modestring = cc.getProperty("workflow.elasticsearch.mode", "none");
		try {
			mode = SearchMode.valueOf(modestring);
		} catch (IllegalArgumentException ie) {
			logger.error("Invalid setting for workflow.elasticsearch.mode: " + modestring + ", supported values are: elasticsearch, memory");
			System.exit(1);
		}

		if((db.equals(DB.dynomite) || db.equals(DB.redis))) {
			String hosts = cc.getProperty("workflow.dynomite.cluster.hosts", null);
			if (hosts == null) {
				System.err.println("Missing dynomite/redis hosts.  Ensure 'workflow.dynomite.cluster.hosts' has been set in the supplied configuration.");
				logger.error("Missing dynomite/redis hosts.  Ensure 'workflow.dynomite.cluster.hosts' has been set in the supplied configuration.");
				System.exit(1);
			}
			String[] hostConfigs = hosts.split(";");

			for (String hostConfig : hostConfigs) {
				String[] hostConfigValues = hostConfig.split(":");
				String host = hostConfigValues[0];
				int port = Integer.parseInt(hostConfigValues[1]);
				String rack = hostConfigValues[2];
				Host dynoHost = new Host(host, port, rack, Status.Up);
				dynoHosts.add(dynoHost);
			}

		} else if (db.equals(DB.memory)) {
			//Create a single shard host supplier
			Host dynoHost = new Host("localhost", 0, cc.getAvailabilityZone(), Status.Up);
			dynoHosts.add(dynoHost);
		}
		init(dynoClusterName, dynoHosts);
	}

	private void init(String dynoClusterName, List<Host> dynoHosts) {
		HostSupplier hs = new HostSupplier() {

			@Override
			public Collection<Host> getHosts() {
				return dynoHosts;
			}
		};

		JedisCommands jedis = null;
		switch (db) {
			case redis:
			case dynomite:

				ConnectionPoolConfigurationImpl cp = new ConnectionPoolConfigurationImpl(dynoClusterName).withTokenSupplier(new TokenMapSupplier() {

					HostToken token = new HostToken(1L, dynoHosts.get(0));

					@Override
					public List<HostToken> getTokens(Set<Host> activeHosts) {
						return Arrays.asList(token);
					}

					@Override
					public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
						return token;
					}
				}).setLocalRack(cc.getAvailabilityZone()).setLocalDataCenter(cc.getRegion());

				jedis = new DynoJedisClient.Builder()
						.withHostSupplier(hs)
						.withApplicationName(cc.getAppId())
						.withDynomiteClusterName(dynoClusterName)
						.withCPConfig(cp)
						.build();

				logger.debug("Starting conductor server using dynomite cluster " + dynoClusterName);

				break;

			case memory:
				jedis = new JedisMock();
				break;
		}

		switch (mode) {
			case memory:

				try {
					Elasticsearch6Embedded.start();
					if (cc.getProperty("workflow.elasticsearch.url", null) == null) {
						System.setProperty("workflow.elasticsearch.url", "http://localhost:9200");
					}
					if (cc.getProperty("workflow.elasticsearch.index.name", null) == null) {
						System.setProperty("workflow.elasticsearch.index.name", "conductor");
					}
				} catch (Exception e) {
					logger.error("Error starting embedded elasticsearch.  Search functionality will be impacted: " + e.getMessage(), e);
				}
				logger.debug("Starting conductor server using in memory data store");
				break;

			case elasticsearch:
			case none:
				break;
		}

		this.sm = new ServerModule(jedis, hs, cc, db);
	}

	public ServerModule getGuiceModule() {
		return sm;
	}

	public synchronized void start(int port, boolean join) throws Exception {

		if (server != null) {
			throw new IllegalStateException("Server is already running");
		}

		try {
			Guice.createInjector(sm);
		} catch (Exception ex) {
			logger.error("Error creating Guice injector " + ex.getMessage(), ex);
			System.exit(-1);
		}

		// Holds handlers
		final HandlerList handlers = new HandlerList();

		// Welcome UI
		ResourceHandler staticHandler = new ResourceHandler();
		staticHandler.setResourceBase(Main.class.getResource("/static").toExternalForm());
		ContextHandler staticContext = new ContextHandler();
		staticContext.setContextPath("/");
		staticContext.setHandler(staticHandler);
		handlers.addHandler(staticContext);

		// Swagger UI
		ResourceHandler swaggerHandler = new ResourceHandler();
		swaggerHandler.setResourceBase(Main.class.getResource("/swagger-ui").toExternalForm());
		ContextHandler swaggerContext = new ContextHandler();
		swaggerContext.setContextPath("/docs");
		swaggerContext.setHandler(swaggerHandler);
		handlers.addHandler(swaggerContext);

		// Main servlet
		ServletContextHandler mainHandler = new ServletContextHandler();
		mainHandler.addFilter(GuiceFilter.class, "/*", EnumSet.allOf(DispatcherType.class));
		mainHandler.addServlet(new ServletHolder(new DefaultServlet()), "/*");
		handlers.addHandler(mainHandler);

		// Time to start
		server = new Server(port);
		server.setHandler(handlers);

		// ONECOND-758: Increase default request and response header size from 8kb to 64kb
		final int max = 64 * 1024;
		for (Connector conn : server.getConnectors()) {
			conn.getConnectionFactory(HttpConnectionFactory.class).getHttpConfiguration().setRequestHeaderSize(max);
			conn.getConnectionFactory(HttpConnectionFactory.class).getHttpConfiguration().setResponseHeaderSize(max);
		}

		server.start();
		logger.info("Started server on http://localhost:" + port + "/");
		MetricService.getInstance().serverStarted();
		try {
			boolean create = Boolean.parseBoolean(cc.getProperty("loadSample", "false"));
			if (create) {
				System.out.println("Creating kitchensink workflow");
				createKitchenSink(port);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

		if (join) {
			server.join();
		}

	}

	public synchronized void stop() throws Exception {
		if (server == null) {
			throw new IllegalStateException("Server is not running.  call #start() method to start the server");
		}
		server.stop();
		server = null;
	}

	private static void createKitchenSink(int port) throws Exception {

		List<TaskDef> taskDefs = new LinkedList<>();
		for (int i = 0; i < 40; i++) {
			taskDefs.add(new TaskDef("task_" + i, "task_" + i, 1, 0));
		}
		taskDefs.add(new TaskDef("search_elasticsearch", "search_elasticsearch", 1, 0));

		Client client = Client.create();
		ObjectMapper om = new ObjectMapper();
		client.resource("http://localhost:" + port + "/api/metadata/taskdefs").type(MediaType.APPLICATION_JSON).post(om.writeValueAsString(taskDefs));

		InputStream stream = Main.class.getResourceAsStream("/kitchensink.json");
		client.resource("http://localhost:" + port + "/api/metadata/workflow").type(MediaType.APPLICATION_JSON).post(stream);

		stream = Main.class.getResourceAsStream("/sub_flow_1.json");
		client.resource("http://localhost:" + port + "/api/metadata/workflow").type(MediaType.APPLICATION_JSON).post(stream);

		String input = "{\"task2Name\":\"task_5\"}";
		client.resource("http://localhost:" + port + "/api/workflow/kitchensink").type(MediaType.APPLICATION_JSON).post(input);

		logger.info("Kitchen sink workflows are created!");
	}
}