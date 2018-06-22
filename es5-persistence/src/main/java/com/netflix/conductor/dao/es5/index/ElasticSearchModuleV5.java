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
package com.netflix.conductor.dao.es5.index;

import java.util.ArrayList;

import javax.inject.Singleton;

import com.netflix.conductor.dao.IndexDAO;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.conductor.core.config.Configuration;


/**
 * @author Viren
 * Provider for the elasticsearch transport client
 */
public class ElasticSearchModuleV5 extends AbstractModule {

	private static Logger log = LoggerFactory.getLogger(ElasticSearchModuleV5.class);
	
	@Provides
	@Singleton
	public CustomHighLevelRestClient getClient(Configuration config) throws Exception {

		String clusterAddress = config.getProperty("workflow.elasticsearch.url", "");
		if(clusterAddress.equals("")) {
			log.warn("workflow.elasticsearch.url is not set.  Indexing will remain DISABLED.");
		}

        Settings settings = Settings.builder().build();

        //TransportClient tc = new PreBuiltTransportClient(settings);
        RestClient restClient = RestClient.builder().build();
        CustomHighLevelRestClient highLevelClient = new CustomHighLevelRestClient(restClient);
        String[] hosts = clusterAddress.split(",");
        ArrayList<HttpHost> hostArray = new ArrayList<>();
        for (String host : hosts) {
            String[] hostparts = host.split(":");
            String hostname = hostparts[0];
            int hostport = 9200;
            if (hostparts.length == 2) hostport = Integer.parseInt(hostparts[1]);
            hostArray.add(new HttpHost(hostname, hostport, "https"));
        }
        restClient.setHosts(hostArray.toArray(new HttpHost[0]));
        return highLevelClient;
    
    }

	@Override
	protected void configure() {
        bind(IndexDAO.class).to(ElasticSearchDAOV5.class);
        bind(CustomHighLevelRestClient.class);
	}
}
