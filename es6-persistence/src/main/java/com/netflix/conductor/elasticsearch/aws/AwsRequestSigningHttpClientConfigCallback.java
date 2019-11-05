package com.netflix.conductor.elasticsearch.aws;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClientBuilder;

public class AwsRequestSigningHttpClientConfigCallback implements RestClientBuilder.HttpClientConfigCallback
{
    private static final String SERVICE_NAME = "es";

    private final ElasticSearchConfiguration configuration;
    private final HttpRequestInterceptor httpRequestInterceptor;

    public AwsRequestSigningHttpClientConfigCallback(ElasticSearchConfiguration configuration)
    {
        this.configuration = configuration;
        this.httpRequestInterceptor = new AWSRequestSigningApacheInterceptor(SERVICE_NAME, buildRequestSigner(), new DefaultAWSCredentialsProviderChain());
    }

    @Override
    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder)
    {
        return httpClientBuilder.addInterceptorLast(httpRequestInterceptor);
    }

    private AWS4Signer buildRequestSigner()
    {
        AWS4Signer requestSigner = new AWS4Signer();
        requestSigner.setServiceName(SERVICE_NAME);
        requestSigner.setRegionName(configuration.getRegion());

        return requestSigner;
    }
}
