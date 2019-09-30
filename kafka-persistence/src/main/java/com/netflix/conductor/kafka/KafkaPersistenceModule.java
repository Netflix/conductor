package com.netflix.conductor.kafka;

import com.google.inject.AbstractModule;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.es5.index.ElasticSearchKafkaDAOV5;
import com.netflix.conductor.dao.es5.index.ElasticSearchRestKafkaDAOV5;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Manan
 * KafkaPersistenceModule.
 */
public class KafkaPersistenceModule extends AbstractModule {

    private final boolean restTransport;

    KafkaPersistenceModule(ElasticSearchConfiguration elasticSearchConfiguration) {

        Set<String> REST_SCHEMAS = new HashSet<>();
        REST_SCHEMAS.add("http");
        REST_SCHEMAS.add("https");

        String esTransport = elasticSearchConfiguration.getURIs().get(0).getScheme();

        this.restTransport = REST_SCHEMAS.contains(esTransport);
    }

    @Override
    protected void configure() {

        if (restTransport) {
            bind(IndexDAO.class).to(ElasticSearchRestKafkaDAOV5.class);
        } else {
            bind(IndexDAO.class).to(ElasticSearchKafkaDAOV5.class);
        }

    }
}
