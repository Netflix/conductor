package com.netflix.conductor.elasticsearch;

import org.elasticsearch.client.RestClientBuilder;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class ElasticSearchRestClientBuilderProviderTest {

    @Test
    public void testGetNoAuth() throws URISyntaxException {
        ElasticSearchConfiguration configuration = Mockito.mock(ElasticSearchConfiguration.class);
        Mockito.when(configuration.getURIs()).thenReturn(Arrays.asList(new URI("https://localhost:9201")));

        ElasticSearchRestClientBuilderProvider provider = new ElasticSearchRestClientBuilderProvider(configuration);
        RestClientBuilder restClientBuilder = provider.get();

        Mockito.verify(configuration, Mockito.atMost(1)).getElasticSearchBasicAuthUsername();
        Mockito.verify(configuration, Mockito.atMost(1)).getElasticSearchBasicAuthPassword();
    }

    @Test
    public void testGetWithAuth() throws URISyntaxException {
        ElasticSearchConfiguration configuration = Mockito.mock(ElasticSearchConfiguration.class);
        Mockito.when(configuration.getElasticSearchBasicAuthUsername()).thenReturn("testuser");
        Mockito.when(configuration.getElasticSearchBasicAuthPassword()).thenReturn("testpassword");
        Mockito.when(configuration.getURIs()).thenReturn(Arrays.asList(new URI("https://localhost:9201")));

        ElasticSearchRestClientBuilderProvider provider = new ElasticSearchRestClientBuilderProvider(configuration);
        RestClientBuilder restClientBuilder = provider.get();

        Mockito.verify(configuration, Mockito.atLeast(2)).getElasticSearchBasicAuthUsername();
        Mockito.verify(configuration, Mockito.atLeast(2)).getElasticSearchBasicAuthPassword();
    }
}
