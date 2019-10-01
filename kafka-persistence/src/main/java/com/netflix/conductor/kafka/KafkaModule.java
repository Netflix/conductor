package com.netflix.conductor.dao.kafka.index; /**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
/**
 *
 */

import com.google.inject.AbstractModule;

import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.es5.index.ElasticSearchKafkaDAOV5;
import com.netflix.conductor.dao.es5.index.ElasticSearchRestKafkaDAOV5;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearchProvider;
import com.netflix.conductor.elasticsearch.es5.EmbeddedElasticSearchV5Provider;

import java.util.HashSet;
import java.util.Set;


/**
 * @author Viren
 * Provider for the elasticsearch index DAO.
 */
public class KafkaModule extends AbstractModule {

    private boolean restTransport;

    public KafkaModule(ElasticSearchConfiguration elasticSearchConfiguration) {

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

        bind(EmbeddedElasticSearchProvider.class).to(EmbeddedElasticSearchV5Provider.class);
    }
}
