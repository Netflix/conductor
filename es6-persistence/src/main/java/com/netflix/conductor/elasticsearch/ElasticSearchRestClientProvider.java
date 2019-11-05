package com.netflix.conductor.elasticsearch;

import com.netflix.conductor.elasticsearch.aws.AwsRequestSigningHttpClientConfigCallback;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.inject.Inject;
import javax.inject.Provider;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticSearchRestClientProvider implements Provider<RestClient> {

    private final ElasticSearchConfiguration configuration;

    @Inject
    public ElasticSearchRestClientProvider(ElasticSearchConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public RestClient get() {
        RestClientBuilder clientBuilder = RestClient.builder(convertToHttpHosts(configuration.getURIs()));
        if (configuration.isAwsIdentityRequestSigningEnabled()) {
            clientBuilder.setHttpClientConfigCallback(new AwsRequestSigningHttpClientConfigCallback(configuration));
        }

        return clientBuilder.build();
    }

    private HttpHost[] convertToHttpHosts(List<URI> hosts) {
        List<HttpHost> list = hosts.stream()
                .map(host -> new HttpHost(host.getHost(), host.getPort(), host.getScheme()))
                .collect(Collectors.toList());

        return list.toArray(new HttpHost[list.size()]);
    }

}
