package com.netflix.conductor.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.apache.http.client.CredentialsProvider;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;


public class ElasticSearchRestClientBuilderProvider implements Provider<RestClientBuilder> {
    private final ElasticSearchConfiguration configuration;

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchRestClientBuilderProvider.class);

    @Inject
    public ElasticSearchRestClientBuilderProvider(ElasticSearchConfiguration configuration) {
        this.configuration = configuration;
    }

    public static String readAsString(String fileName)throws Exception
    {
        String data = "";
        data = new String(Files.readAllBytes(Paths.get(fileName)));
        return data;
    }

    @Override
    public RestClientBuilder get() {

        try {
            HttpClient client = ElasticSearchUtils.noSSLClient();
        } catch (GeneralSecurityException e) {
            e.printStackTrace();
        }

        RestClientBuilder builder = RestClient.builder(convertToHttpHosts(configuration.getURIs()));

        if (configuration.getElasticSearchSSLEnabled()) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(configuration.getElasticSearchBasicAuthUsername(), configuration.getElasticSearchBasicAuthPassword()));

            String keyStorePass = configuration.getJavaKeystorePassword();

            try {
                InputStream is = Files.newInputStream(Paths.get(configuration.getJavaKeystorePath()));
                KeyStore truststore = KeyStore.getInstance("jks");
                truststore.load(is, keyStorePass.toCharArray());
                SSLContextBuilder sslBuilder = SSLContexts.custom().loadTrustMaterial(truststore, null);

                SSLContext sslContext = sslBuilder.build();

                return builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                .setSSLContext(sslContext);
                    }
                });
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }

        return builder;
    }


    private HttpHost[] convertToHttpHosts(List<URI> hosts) {
        List<HttpHost> list = hosts.stream()
                .map(host -> new HttpHost(host.getHost(), host.getPort(), host.getScheme()))
                .collect(Collectors.toList());

        return list.toArray(new HttpHost[list.size()]);
    }
}
