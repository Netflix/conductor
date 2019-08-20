/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.jetty.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.servlet.GuiceFilter;
import com.netflix.conductor.bootstrap.Main;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.service.Lifecycle;
import com.sun.jersey.api.client.Client;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Viren
 */
public class JettyServer implements Lifecycle {

    private static Logger logger = LoggerFactory.getLogger(JettyServer.class);

    private final int port;
    private final boolean join;

    private Server server;


    public JettyServer(int port, boolean join) {
        this.port = port;
        this.join = join;
    }


    @Override
    public synchronized void start() throws Exception {

        if (server != null) {
            throw new IllegalStateException("Server is already running");
        }

        this.server = new Server(new QueuedThreadPool(50, 500));
        ServerConnector connector = new ServerConnector(this.server, new HttpConnectionFactory());
        connector.setPort(port);

        this.server.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler();
        context.addFilter(GuiceFilter.class, "/*", EnumSet.allOf(DispatcherType.class));
        context.setWelcomeFiles(new String[]{"index.html"});

        server.setHandler(context);

        server.start();
        System.out.println("Started server on http://localhost:" + port + "/");
        try {
            boolean create = Boolean.getBoolean("loadSample");
            if (create) {
                System.out.println("Creating kitchensink workflow");
                createKitchenSink(port);
            }
        } catch (Exception e) {
            logger.error("Error loading sample!", e);
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
        Client client = Client.create();
        ObjectMapper objectMapper = new ObjectMapper();


        List<TaskDef> taskDefs = new LinkedList<>();
        for (int i = 0; i < 40; i++) {
            taskDefs.add(new TaskDef("task_" + i, "task_" + i, 1, 0));
        }
        taskDefs.add(new TaskDef("search_elasticsearch", "search_elasticsearch", 1, 0));

        client.resource("http://localhost:" + port + "/api/metadata/taskdefs").type(MediaType.APPLICATION_JSON).post(objectMapper.writeValueAsString(taskDefs));

        /*
         * Kitchensink example (stored workflow with stored tasks)
         */
        InputStream stream = Main.class.getResourceAsStream("/kitchensink.json");
        client.resource("http://localhost:" + port + "/api/metadata/workflow").type(MediaType.APPLICATION_JSON).post(stream);

        stream = Main.class.getResourceAsStream("/sub_flow_1.json");
        client.resource("http://localhost:" + port + "/api/metadata/workflow").type(MediaType.APPLICATION_JSON).post(stream);

        Map<String, Object> payload = ImmutableMap.of("task2Name", "task_5");
        String payloadStr = objectMapper.writeValueAsString(payload);
        client.resource("http://localhost:" + port + "/api/workflow/kitchensink").type(MediaType.APPLICATION_JSON).post(payloadStr);

        logger.info("Kitchen sink workflow is created!");

        /*
         * Kitchensink example with ephemeral workflow and stored tasks
         */
        InputStream ephemeralInputStream = Main.class.getResourceAsStream("/kitchenSink-ephemeralWorkflowWithStoredTasks.json");
        client.resource("http://localhost:" + port + "/api/workflow/").type(MediaType.APPLICATION_JSON).post(ephemeralInputStream);
        logger.info("Ephemeral Kitchen sink workflow with stored tasks is created!");

        /*
         * Kitchensink example with ephemeral workflow and ephemeral tasks
         */
        ephemeralInputStream = Main.class.getResourceAsStream("/kitchenSink-ephemeralWorkflowWithEphemeralTasks.json");
        client.resource("http://localhost:" + port + "/api/workflow/").type(MediaType.APPLICATION_JSON).post(ephemeralInputStream);
        logger.info("Ephemeral Kitchen sink workflow with ephemeral tasks is created!");

    }
}
