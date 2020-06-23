package com.netflix.conductor.health;

import com.netflix.runtime.health.api.Health;
import com.netflix.runtime.health.api.HealthIndicator;
import com.netflix.runtime.health.api.HealthIndicatorCallback;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class ElasticSearchHealthIndicator implements HealthIndicator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchHealthIndicator.class);

    private final Client esClient;

    @Inject
    public ElasticSearchHealthIndicator(Client esClient) {
        this.esClient = esClient;
        LOGGER.info("Health Indicator is Ready");
   }

    @Override
    public void check(HealthIndicatorCallback healthIndicatorCallback) {
        LOGGER.debug("Checking Health");
        try {
            ClusterHealthResponse health = this.esClient.admin().cluster().prepareHealth().get();

            if (health.isTimedOut()) {
                healthIndicatorCallback.inform(
                    Health
                        .unhealthy()
                        .withDetail("clusterHealth", health)
                        .build()
                );
            } else if (health.getStatus().equals(ClusterHealthStatus.GREEN)
                || health.getStatus().equals(ClusterHealthStatus.YELLOW)) { // Allow YELLOW status (single node)
                healthIndicatorCallback.inform(Health.healthy().build());
            } else {
                healthIndicatorCallback.inform(Health.unhealthy().withDetail("clusterHealth", health).build());
            }
        } catch (Exception e) {
            healthIndicatorCallback.inform(Health.unhealthy().withException(e).build());
        }
    }
}
