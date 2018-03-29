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
package com.netflix.conductor.contribs.http;

import com.netflix.conductor.core.config.Configuration;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.config.DefaultApacheHttpClient4Config;
import org.apache.http.impl.conn.PoolingClientConnectionManager;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * @author Viren
 * Provider for Jersey Client.  This class provides an 
 */
@Singleton
public class RestClientManager {

	private final Client defaultClient;

	@Inject
	@SuppressWarnings("deprecation")
	public RestClientManager(Configuration config) {
		// org.apache.http.impl.conn.PoolingClientConnectionManager
		String className = PoolingClientConnectionManager.class.getName();

		// Increase max total connection to 200
		int maxTotal = config.getIntProperty( className + ".maxTotal", 200);

		// Increase default max connection per route to 20
		int defaultMaxPerRoute = config.getIntProperty(className + ".defaultMaxPerRoute", 20);

		PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
		cm.setMaxTotal(maxTotal);
		cm.setDefaultMaxPerRoute(defaultMaxPerRoute);

		ClientConfig apacheConfig = new DefaultApacheHttpClient4Config();
		apacheConfig.getProperties().put(DefaultApacheHttpClient4Config.PROPERTY_CONNECTION_MANAGER, cm);

		// create the client
		defaultClient = ApacheHttpClient4.create(apacheConfig);
	}

	public Client getClient(Input input) {
		return defaultClient;
	}
}
