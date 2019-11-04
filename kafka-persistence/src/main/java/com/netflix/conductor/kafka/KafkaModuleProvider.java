package com.netflix.conductor.kafka;

import com.google.inject.AbstractModule;
import com.netflix.conductor.dao.ProducerDAO;
import com.netflix.conductor.dao.kafka.index.KafkaModule;
import com.netflix.conductor.dao.kafka.index.producer.KafkaProducer;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.SystemPropertiesElasticSearchConfiguration;

public class KafkaModuleProvider extends AbstractModule {

    public KafkaModuleProvider() {
    }

    @Override
    protected void configure() {
        ElasticSearchConfiguration configuration = new SystemPropertiesElasticSearchConfiguration();
        if (configuration.getKafkaIndexEnable()) {
            bind(ProducerDAO.class).to(KafkaProducer.class);
            install(new KafkaModule(configuration));
        }
    }
}


