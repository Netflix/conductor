/*
 * Copyright 2022 Netflix, Inc.
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
package com.netflix.conductor.tasks.http.providers;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Optional;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.netflix.conductor.tasks.http.HttpTask;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

/**
 * Provider for a customized RestTemplateBuilder. This class provides a default {@link
 * RestTemplateBuilder} which can be configured or extended as needed.
 */
@Component
public class DefaultRestTemplateProvider implements RestTemplateProvider {

  private final ThreadLocal<RestTemplate> threadLocalRestTemplate;

  private final int defaultReadTimeout;
  private final int defaultConnectTimeout;

  @Autowired
  public DefaultRestTemplateProvider(
      @Value("${conductor.tasks.http.readTimeout:150ms}") Duration readTimeout,
      @Value("${conductor.tasks.http.connectTimeout:100ms}") Duration connectTimeout) {
    this.threadLocalRestTemplate = ThreadLocal.withInitial(RestTemplate::new);
    this.defaultReadTimeout = (int) readTimeout.toMillis();
    this.defaultConnectTimeout = (int) connectTimeout.toMillis();
  }

  @Override
  public @NonNull RestTemplate getRestTemplate(@NonNull HttpTask.Input input) {
    RestTemplate restTemplate = threadLocalRestTemplate.get();
    HttpComponentsClientHttpRequestFactory requestFactory =
        new HttpComponentsClientHttpRequestFactory();
    requestFactory.setConnectTimeout(
        Optional.ofNullable(input.getConnectionTimeOut()).orElse(defaultConnectTimeout));
    requestFactory.setReadTimeout(
        Optional.ofNullable(input.getReadTimeOut()).orElse(defaultReadTimeout));
    try {
      requestFactory.setHttpClient(createAcceptsUntrustedCertsHttpClient());
    } catch (Exception e) {
      e.printStackTrace();
    }
    restTemplate.setRequestFactory(requestFactory);
    return restTemplate;
  }

  private CloseableHttpClient createAcceptsUntrustedCertsHttpClient()
      throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();

    // set up a Trust Strategy that allows all certificates.
    SSLContext sslContext =
        new SSLContextBuilder()
            .loadTrustMaterial(
                null,
                new TrustStrategy() {
                  @Override
                  public boolean isTrusted(X509Certificate[] arg0, String arg1)
                      throws CertificateException {
                    return true;
                  }
                })
            .build();
    httpClientBuilder.setSSLContext(sslContext);

    // don't check Hostnames, either.
    //      -- use SSLConnectionSocketFactory.getDefaultHostnameVerifier(), if you don't want to
    // weaken
    HostnameVerifier hostnameVerifier = NoopHostnameVerifier.INSTANCE;

    // here's the special part:
    //      -- need to create an SSL Socket Factory, to use our weakened "trust strategy";
    //      -- and create a Registry, to register it.
    //
    SSLConnectionSocketFactory sslSocketFactory =
        new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
    Registry<ConnectionSocketFactory> socketFactoryRegistry =
        RegistryBuilder.<ConnectionSocketFactory>create()
            .register("http", PlainConnectionSocketFactory.getSocketFactory())
            .register("https", sslSocketFactory)
            .build();

    // now, we create connection-manager using our Registry.
    //      -- allows multi-threaded use
    PoolingHttpClientConnectionManager connMgr =
        new PoolingHttpClientConnectionManager(socketFactoryRegistry);
    connMgr.setMaxTotal(200);
    connMgr.setDefaultMaxPerRoute(100);
    httpClientBuilder.setConnectionManager(connMgr);

    CloseableHttpClient client = httpClientBuilder.build();

    return client;
  }
}
