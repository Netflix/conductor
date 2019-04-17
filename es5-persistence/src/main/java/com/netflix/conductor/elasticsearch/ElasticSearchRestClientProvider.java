package com.netflix.conductor.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.inject.Inject;
import javax.inject.Provider;
import java.net.URI;
import java.util.List;
import com.google.common.base.Supplier;
import java.util.stream.Collectors;

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
