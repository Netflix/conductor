/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package com.netflix.conductor.dao.esrest.index;

import java.util.List;
import java.util.ArrayList;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import javax.inject.Singleton;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.conductor.core.config.Configuration;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.base.*;
import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;

/**
 * @author Viren
 * Provider for the elasticsearch transport client
 */
public class ElasticsearchModule extends AbstractModule {

	private static Logger log = LoggerFactory.getLogger(ElasticsearchModule.class);
        private static final String DEFAULT_USER = "elastic";
        private static final String DEFAULT_PASSWORD = "changeme";
        private static final String SERVICE = "es";
        private static final String region = System.getenv("AWS_REGION");

	@Provides
	@Singleton
	public ElasticSearchRestClient getClient(Configuration config) throws Exception {

		String clusterAddress = config.getProperty("workflow.elasticsearch.url", "");
		if(clusterAddress.equals("")) {
			log.warn("workflow.elasticsearch.url is not set.  Indexing will remain DISABLED.");
		}
        boolean isAwsEs = false; 
        String awsEs = config.getProperty("workflow.elasticsearch.aws", "");
        if(awsEs.equalsIgnoreCase("true")) {
                log.info("workflow.elasticsearch.aws is enabled, requests will be signed with AWS keys.");
                isAwsEs = true;
        }


        String credentials = config.getProperty("elasticsearch.xpack.security.user", "");
        if(credentials.equals("")) {
            credentials = DEFAULT_USER + ":" + DEFAULT_PASSWORD;
            log.warn("elasticsearch.xpack.security.user is not set.  Will try using default values: " + credentials);
        }

        String[] hosts = clusterAddress.split(",");
        List<HttpHost> hostList = new ArrayList<HttpHost>();
        for (String host : hosts) {
            String[] hostparts = host.split(":");
            String hostname = hostparts[0];
            int hostport = 9200;
            if (hostparts.length == 2) hostport = Integer.parseInt(hostparts[1]);
            if (isAwsEs) 
                    hostList.add(new HttpHost(hostname, hostport, "https"));
	    else 
		    hostList.add(new HttpHost(hostname, hostport, "http"));
            log.info("Adding Host: " + hostname + ":" + hostport);
        }
        
        HttpHost[] test = hostList.toArray(new HttpHost[hostList.size()]);
        for (HttpHost h : test) {
        	log.info(h.toString());
        }
        RestClientBuilder lowLevelRestClientBuilder;
        if (isAwsEs) {
                // Get the Default AWS Credential Provider and add a Request Interceptor for signing the requests with AWS keys
                final Supplier<LocalDateTime> clock = () -> LocalDateTime.now(ZoneOffset.UTC);
                DefaultAWSCredentialsProviderChain awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
                final AWSSigner awsSigner = new AWSSigner(awsCredentialsProvider, region, SERVICE, clock);
                final AWSSigningRequestInterceptor requestInterceptor = new AWSSigningRequestInterceptor(awsSigner);
                lowLevelRestClientBuilder = RestClient.builder(hostList.toArray(new HttpHost[hostList.size()])).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                         @Override
                              public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                     return httpClientBuilder.addInterceptorLast(requestInterceptor);
                              }
                  });
        } else {
                lowLevelRestClientBuilder = RestClient.builder(hostList.toArray(new HttpHost[hostList.size()]));
        }
        RestClient lowLevelRestClient = lowLevelRestClientBuilder.build();

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(lowLevelRestClient);
        
        ElasticSearchRestClient client = new ElasticSearchRestClient(restHighLevelClient, lowLevelRestClient);
        
        return client;
    
    }

	@Override
	protected void configure() {
		
	}
}
