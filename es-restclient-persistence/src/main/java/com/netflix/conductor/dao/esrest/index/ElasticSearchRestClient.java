/**
 * Copyright 2017 Netflix, Inc.
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

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;


// This class is used to provide a way to access both the RestHighLevelClient and RestClient (low level client).
// This has been fixed with version 6.0 (there is a method getLowLevelClient) but it is not available in the 5.x versions.
public class ElasticSearchRestClient {

	ElasticSearchRestClient(RestHighLevelClient highLevelClient, RestClient lowLevelClient) {
		this.highLevelClient = highLevelClient;
		this.lowLevelClient = lowLevelClient;
	}
	
	public RestHighLevelClient getHighLevelClient() {
		return highLevelClient;
	}

	public void setHighLevelClient(RestHighLevelClient highLevelClient) {
		this.highLevelClient = highLevelClient;
	}

	public RestClient getLowLevelClient() {
		return lowLevelClient;
	}

	public void setLowLevelClient(RestClient lowLevelClient) {
		this.lowLevelClient = lowLevelClient;
	}

	private RestHighLevelClient highLevelClient;
	
	private RestClient lowLevelClient;
}
