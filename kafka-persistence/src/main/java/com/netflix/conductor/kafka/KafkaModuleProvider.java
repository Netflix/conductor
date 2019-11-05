package com.netflix.conductor.kafka;

import com.google.inject.AbstractModule;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.KafkaDAO;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearchProvider;
import com.netflix.conductor.elasticsearch.SystemPropertiesElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.es5.EmbeddedElasticSearchV5Provider;

public class KafkaModuleProvider extends AbstractModule {

    public KafkaModuleProvider() {
    }

    @Override
    protected void configure() {
        ElasticSearchConfiguration configuration = new SystemPropertiesElasticSearchConfiguration();
        if (configuration.getKafkaIndexEnable()) {
            bind(IndexDAO.class).to(KafkaDAO.class);
            bind(EmbeddedElasticSearchProvider.class).to(EmbeddedElasticSearchV5Provider.class);
        }
    }
}


