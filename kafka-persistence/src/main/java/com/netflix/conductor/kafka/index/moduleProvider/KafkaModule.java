package com.netflix.conductor.kafka.index.moduleProvider;

import com.google.inject.AbstractModule;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.KafkaDAO;
import com.netflix.conductor.dao.ProducerDAO;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearchProvider;
import com.netflix.conductor.elasticsearch.es5.EmbeddedElasticSearchV5Provider;
import com.netflix.conductor.kafka.index.configuration.SystemPropertiesKafkaConfiguration;
import com.netflix.conductor.kafka.index.producer.KafkaProducer;

public class KafkaModule extends AbstractModule {

    public KafkaModule() {
    }

    @Override
    protected void configure() {
        SystemPropertiesKafkaConfiguration configuration = new SystemPropertiesKafkaConfiguration();
        if (configuration.getKafkaIndexEnable()) {
            bind(IndexDAO.class).to(KafkaDAO.class);
            bind(ProducerDAO.class).to(KafkaProducer.class);
        }
    }
}


