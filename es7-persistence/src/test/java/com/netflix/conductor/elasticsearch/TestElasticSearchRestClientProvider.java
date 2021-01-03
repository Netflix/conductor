package com.netflix.conductor.elasticsearch;

import org.elasticsearch.client.RestClient;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TestElasticSearchRestClientProvider {
    @Test
    public void testGetRestClient() throws URISyntaxException {
        ElasticSearchConfiguration configuration = Mockito.mock(ElasticSearchConfiguration.class);
        Mockito.when(configuration.getElasticsearchRestClientConnectionRequestTimeout()).thenReturn(30000);
        Mockito.when(configuration.getURIs()).thenReturn(Arrays.asList(new URI("https://localhost:9201")));

        ElasticSearchRestClientProvider elasticSearchRestClientProvider = new ElasticSearchRestClientProvider(configuration);
        RestClient restClient = elasticSearchRestClientProvider.get();

        assertEquals(restClient.getNodes().get(0).getHost().getHostName(), "localhost");
        assertEquals(restClient.getNodes().get(0).getHost().getSchemeName(),"https");
        assertEquals(restClient.getNodes().get(0).getHost().getPort(), 9201);
    }
}
