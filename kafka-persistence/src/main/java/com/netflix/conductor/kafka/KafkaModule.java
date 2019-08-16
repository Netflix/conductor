package com.netflix.conductor.kafka;

import com.google.inject.AbstractModule;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.KafkaProducerDAO;
import com.netflix.conductor.dao.kafka.index.KafkaProducer;


public class KafkaModule extends AbstractModule {

    private Configuration configuration;

    public KafkaModule(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void configure() {
        if (this.configuration.getBoolProperty("workflow.kafka.index.enable", false)) {
            install(new KafkaPersistenceModule());
        }
        bind(KafkaProducerDAO.class).to(KafkaProducer.class);
    }
}


