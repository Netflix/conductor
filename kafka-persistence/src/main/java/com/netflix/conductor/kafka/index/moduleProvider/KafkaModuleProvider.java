package com.netflix.conductor.kafka.index.moduleProvider;

import com.google.inject.AbstractModule;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.KafkaDAO;
import com.netflix.conductor.kafka.index.configuration.SystemPropertiesKafkaConfiguration;

public class KafkaModuleProvider extends AbstractModule {

    public KafkaModuleProvider() {
    }

    @Override
    protected void configure() {
        SystemPropertiesKafkaConfiguration configuration = new SystemPropertiesKafkaConfiguration();
        if (configuration.getKafkaIndexEnable()) {
            bind(IndexDAO.class).to(KafkaDAO.class);
        }
    }
}


