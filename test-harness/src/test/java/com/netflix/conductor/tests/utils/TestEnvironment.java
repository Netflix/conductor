package com.netflix.conductor.tests.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.conductor.core.config.Configuration.AVAILABILITY_ZONE_DEFAULT_VALUE;
import static com.netflix.conductor.core.config.Configuration.AVAILABILITY_ZONE_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.DB_DEFAULT_VALUE;
import static com.netflix.conductor.core.config.Configuration.DB_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.MAX_TASK_INPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.MAX_TASK_OUTPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.MAX_WORKFLOW_INPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.MAX_WORKFLOW_OUTPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.REGION_DEFAULT_VALUE;
import static com.netflix.conductor.core.config.Configuration.REGION_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.TASK_INPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.TASK_OUTPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.WORKFLOW_INPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.WORKFLOW_NAMESPACE_PREFIX_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.WORKFLOW_OUTPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME;
import static com.netflix.conductor.elasticsearch.ElasticSearchConfiguration.ELASTIC_SEARCH_INDEX_NAME_DEFAULT_VALUE;
import static com.netflix.conductor.elasticsearch.ElasticSearchConfiguration.ELASTIC_SEARCH_INDEX_NAME_PROPERTY_NAME;
import static com.netflix.conductor.sql.SQLConfiguration.CONNECTION_POOL_MAX_SIZE_PROPERTY_NAME;
import static com.netflix.conductor.sql.SQLConfiguration.CONNECTION_POOL_MINIMUM_IDLE_PROPERTY_NAME;

public class TestEnvironment {
    protected static final Logger logger = LoggerFactory.getLogger(TestEnvironment.class);

    private TestEnvironment() {}

    private static void setupSystemProperties() {

        logger.debug("setting system properties");

        System.setProperty(REGION_PROPERTY_NAME, REGION_DEFAULT_VALUE);
        System.setProperty(AVAILABILITY_ZONE_PROPERTY_NAME, AVAILABILITY_ZONE_DEFAULT_VALUE);
        System.setProperty(ELASTIC_SEARCH_INDEX_NAME_PROPERTY_NAME, ELASTIC_SEARCH_INDEX_NAME_DEFAULT_VALUE);
        System.setProperty(WORKFLOW_NAMESPACE_PREFIX_PROPERTY_NAME, "integration-test");
        System.setProperty(DB_PROPERTY_NAME, DB_DEFAULT_VALUE);

        System.setProperty(REGION_PROPERTY_NAME, REGION_DEFAULT_VALUE);
        System.setProperty(AVAILABILITY_ZONE_PROPERTY_NAME, AVAILABILITY_ZONE_DEFAULT_VALUE);

        System.setProperty(WORKFLOW_INPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME, "10");
        System.setProperty(MAX_WORKFLOW_INPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME, "10240");

        System.setProperty(WORKFLOW_OUTPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME, "10");
        System.setProperty(MAX_WORKFLOW_OUTPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME, "10240");

        System.setProperty(TASK_INPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME, "1");
        System.setProperty(MAX_TASK_INPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME, "10240");

        System.setProperty(TASK_OUTPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME, "10");
        System.setProperty(MAX_TASK_OUTPUT_PAYLOAD_THRESHOLD_KB_PROPERTY_NAME, "10240");

        // jdbc properties
        System.setProperty(CONNECTION_POOL_MAX_SIZE_PROPERTY_NAME, "8");
        System.setProperty(CONNECTION_POOL_MINIMUM_IDLE_PROPERTY_NAME, "300000");

    }

    public static void setup() {
        setupSystemProperties();
    }

    public static void teardown() {
        System.setProperties(null);
    }
}
