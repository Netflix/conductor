package com.netflix.conductor.kafka;

import com.google.inject.AbstractModule;
import com.netflix.conductor.dao.ProducerDAO;
import com.netflix.conductor.dao.kafka.index.producer.KafkaProducer;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearchProvider;
import com.netflix.conductor.elasticsearch.SystemPropertiesElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.es5.EmbeddedElasticSearchV5Provider;


public class KafkaModule extends AbstractModule {

    public KafkaModule() {
    }

    @Override
    protected void configure() {
        ElasticSearchConfiguration configuration = new SystemPropertiesElasticSearchConfiguration();
        if (configuration.getKafkaIndexEnable()) {
            install(new KafkaPersistenceModule(configuration));
        }
        bind(ProducerDAO.class).to(KafkaProducer.class);
        bind(EmbeddedElasticSearchProvider.class).to(EmbeddedElasticSearchV5Provider.class);
    }
}


