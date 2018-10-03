package com.netflix.conductor.dao.es6rest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;

public class Elasticsearch6Embedded {
    private static final Logger logger = LoggerFactory.getLogger(Elasticsearch6Embedded.class);
    private static final String ES_PATH_DATA = "path.data";
    private static final String ES_PATH_HOME = "path.home";

    private static final String DEFAULT_CLUSTER_NAME = "conductor";
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 9200;

    private static File dataDir;
    private static File homeDir;

    private static Node instance;

    private static class PluginConfigurableNode extends Node {
        PluginConfigurableNode(Settings preparedSettings, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(InternalSettingsPreparer.prepareEnvironment(preparedSettings, null), classpathPlugins);
        }
    }

    public static void start() throws Exception {
        dataDir = Files.createTempDirectory(DEFAULT_CLUSTER_NAME + "_" + System.currentTimeMillis() + "-data").toFile();
        homeDir = Files.createTempDirectory(DEFAULT_CLUSTER_NAME + "_" + System.currentTimeMillis() + "-home").toFile();

        Settings settings = Settings.builder()
                .put("cluster.name", DEFAULT_CLUSTER_NAME)
                .put("http.host", DEFAULT_HOST)
                .put("http.port", DEFAULT_PORT)
                .put(ES_PATH_DATA, dataDir.getAbsolutePath())
                .put(ES_PATH_HOME, homeDir.getAbsolutePath())
                .put("node.data", true)
                .put("transport.type", "netty4").build();

        instance = new PluginConfigurableNode(settings, Collections.singletonList(Netty4Plugin.class));
        instance.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                instance.close();
            } catch (IOException e) {
                logger.error("Error closing ElasticSearch");
            }
        }));
    }

    public static synchronized void stop() throws Exception {
        try {
            instance.close();
        } finally {
            dataDir.delete();
            homeDir.delete();
        }
    }

}
