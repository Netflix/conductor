/*
 * Copyright 2020 Netflix, Inc.
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
package com.netflix.conductor.es6.config;

import java.net.InetAddress;
import java.net.URL;
import java.util.List;
import java.util.Optional;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.es6.dao.index.ElasticSearchDAOV6;
import com.netflix.conductor.es6.dao.index.ElasticSearchRestDAOV6;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ElasticSearchProperties.class)
@Conditional(ElasticSearchConditions.ElasticSearchV6Enabled.class)
public class ElasticSearchV6Configuration {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchV6Configuration.class);

    @Bean
    public Client client(ElasticSearchProperties properties) {
        Settings settings =
                Settings.builder()
                        .put("client.transport.ignore_cluster_name", true)
                        .put("client.transport.sniff", true)
                        .build();

        TransportClient transportClient = new PreBuiltTransportClient(settings);

        List<URL> clusterAddresses = properties.toURLs();

        if (clusterAddresses.isEmpty()) {
            log.warn("workflow.elasticsearch.url is not set.  Indexing will remain DISABLED.");
        }
        for (URL hostAddress : clusterAddresses) {
            int port = Optional.ofNullable(hostAddress.getPort()).orElse(9200);
            try {
                transportClient.addTransportAddress(
                        new TransportAddress(InetAddress.getByName(hostAddress.getHost()), port));
            } catch (Exception e) {
                throw new RuntimeException("Invalid host" + hostAddress.getHost(), e);
            }
        }
        return transportClient;
    }

    @Bean
    public RestClient restClient(ElasticSearchProperties properties) {
        RestClientBuilder restClientBuilder =
                RestClient.builder(convertToHttpHosts(properties.toURLs()));
        if (properties.getRestClientConnectionRequestTimeout() > 0) {
            restClientBuilder.setRequestConfigCallback(
                    requestConfigBuilder ->
                            requestConfigBuilder.setConnectionRequestTimeout(
                                    properties.getRestClientConnectionRequestTimeout()));
        }
        return restClientBuilder.build();
    }

    @Bean
    public RestClientBuilder restClientBuilder(ElasticSearchProperties properties) {
        return RestClient.builder(convertToHttpHosts(properties.toURLs()));
    }

    @Bean
    public IndexDAO es6IndexDAO(
            RestClientBuilder restClientBuilder,
            Client client,
            ElasticSearchProperties properties,
            ObjectMapper objectMapper) {
        String url = properties.getUrl();
        if (url.startsWith("http") || url.startsWith("https")) {
            return new ElasticSearchRestDAOV6(restClientBuilder, properties, objectMapper);
        } else {
            return new ElasticSearchDAOV6(client, properties, objectMapper);
        }
    }

    private HttpHost[] convertToHttpHosts(List<URL> hosts) {
        return hosts.stream()
                .map(host -> new HttpHost(host.getHost(), host.getPort(), host.getProtocol()))
                .toArray(HttpHost[]::new);
    }
}
