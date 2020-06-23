package com.netflix.conductor.health;

import com.google.inject.AbstractModule;
import com.netflix.runtime.health.guice.HealthModule;

public class ElasticSearchHealthModule extends AbstractModule {

    @Override
    protected void configure() {

        install(new HealthModule() {
            @Override
            protected void configureHealth() {
                super.configureHealth();
                bindAdditionalHealthIndicator().to(ElasticSearchHealthIndicator.class);
            }
        });
    }
}
