/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.elasticsearch.es6;

import com.google.inject.AbstractModule;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.es6.index.ElasticSearchDAOV6;
import com.netflix.conductor.elasticsearch.ElasticSearchModule;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearchProvider;

public class ElasticSearchV6Module extends AbstractModule {

    @Override
    protected void configure() {
        install(new ElasticSearchModule());
        bind(IndexDAO.class).to(ElasticSearchDAOV6.class);
        bind(EmbeddedElasticSearchProvider.class).to(EmbeddedElasticSearchV6Provider.class);
    }
}
