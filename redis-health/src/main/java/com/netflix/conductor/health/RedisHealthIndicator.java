package com.netflix.conductor.health;

import com.netflix.runtime.health.api.Health;
import com.netflix.runtime.health.api.HealthIndicator;
import com.netflix.runtime.health.api.HealthIndicatorCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.commands.JedisCommands;

import javax.inject.Inject;

public class RedisHealthIndicator implements HealthIndicator {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisHealthIndicator.class);

    private final JedisCommands jedisClient;

    @Inject
    public RedisHealthIndicator(JedisCommands jedisClient) {
        this.jedisClient = jedisClient;
        LOGGER.info("Health Indicator is Ready");
    }

    @Override
    public void check(HealthIndicatorCallback healthIndicatorCallback) {
        LOGGER.debug("Checking Health");
        try {
            this.jedisClient.exists(""); // Key can be whatever. Only used to test connectivity.
            healthIndicatorCallback.inform(Health.healthy().build());
        } catch (Exception e) {
            healthIndicatorCallback.inform(Health.unhealthy().withException(e).build());
        }
    }
}
