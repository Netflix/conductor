/*
 * Copyright 2020 Netflix, Inc.
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
package com.netflix.conductor.elasticsearch;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import com.google.common.base.Supplier;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClientBuilder;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchRestClientProvider implements Provider<RestClient> {
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchRestClientProvider.class);
    private final ElasticSearchConfiguration configuration;

    // AWS related env variables which is provided during pod initialization
    private static final String SERVICE = "es";
    private static final String region = System.getenv("AWS_REGION");

    @Inject
    public ElasticSearchRestClientProvider(ElasticSearchConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public RestClient get() {
        HttpHost[] httpHosts = convertToHttpHosts(configuration.getURIs());

        // If AWS ES property is true, then we return a RestClient which signs the request with AWS keys
        if( configuration.isAwsEs()){

            logger.info("workflow.elasticsearch.aws is enabled, requests would be signed with AWS keys.");

            // Get the Default AWS Credential Provider and add a Request Interceptor for signing the requests with AWS keys
            final Supplier<LocalDateTime> clock = () -> LocalDateTime.now(ZoneOffset.UTC);
            DefaultAWSCredentialsProviderChain awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
            final AWSSigner awsSigner = new AWSSigner(awsCredentialsProvider, region, SERVICE, clock);
            final AWSSigningRequestInterceptor requestInterceptor = new AWSSigningRequestInterceptor(awsSigner);
            RestClientBuilder lowLevelRestClientBuilder = RestClient.builder(httpHosts).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                     @Override
                          public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                 return httpClientBuilder.addInterceptorLast(requestInterceptor);
                          }
              });

              return lowLevelRestClientBuilder.build();
        }

        return RestClient.builder(httpHosts).build();
    }

    private HttpHost[] convertToHttpHosts(List<URI> hosts) {
        List<HttpHost> list = hosts.stream()
                .map(host -> new HttpHost(host.getHost(), host.getPort(), host.getScheme()))
                .collect(Collectors.toList());

        return list.toArray(new HttpHost[list.size()]);
    }
}
