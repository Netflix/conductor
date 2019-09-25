package com.netflix.conductor.kafka;

import com.google.inject.AbstractModule;
import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import com.netflix.conductor.dao.ProducerDAO;
import com.netflix.conductor.dao.kafka.index.producer.KafkaProducer;


public class KafkaModule extends AbstractModule {

    public KafkaModule() {
    }

    @Override
    protected void configure() {
        SystemPropertiesConfiguration configuration = new SystemPropertiesConfiguration();
        if (configuration.getKafkaIndexEnable()) {
            bind(ProducerDAO.class).to(KafkaProducer.class);
        }
    }
}


