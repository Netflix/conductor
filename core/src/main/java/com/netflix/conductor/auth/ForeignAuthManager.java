/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.config.Configuration;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
public class ForeignAuthManager {
	private final ObjectMapper mapper = new ObjectMapper();
	private final Configuration config;

	@Inject
	public ForeignAuthManager(Configuration config) {
		this.config = config;
	}

	public AuthResponse authorize(String party, Map<String, String> authorizeHeaders) throws Exception {
		List<NameValuePair> params = new ArrayList<>();
		params.add(new BasicNameValuePair("grant_type", getProperty("conductor_auth_" + party.toLowerCase() + "_grant_type", "client_credentials")));
		params.add(new BasicNameValuePair("client_id", getProperty("conductor_auth_" + party.toLowerCase() + "_client_id", null)));
		params.add(new BasicNameValuePair("client_secret", getProperty("conductor_auth_" + party.toLowerCase() + "_client_secret", null)));
		if (MapUtils.isNotEmpty(authorizeHeaders)) {
			authorizeHeaders.forEach((k, v) -> params.add(new BasicNameValuePair(k, v)));
		}
		String url = getProperty("conductor_auth_" + party.toLowerCase() + "_url", null);
		try (CloseableHttpClient client = HttpClientBuilder.create().setRedirectStrategy(new LaxRedirectStrategy()).build()) {
			HttpPost httpPost = new HttpPost(url);
			httpPost.setEntity(new UrlEncodedFormEntity(params));
			CloseableHttpResponse response = client.execute(httpPost);

			HttpEntity httpEntity = response.getEntity();
			if (response.getStatusLine().getStatusCode() == 200) {
				return mapper.readValue(httpEntity.getContent(), AuthResponse.class);
			} else {
				String entity = EntityUtils.toString(httpEntity);
				if (StringUtils.isEmpty(entity)) {
					return new AuthResponse("no content", "server did not return body");
				}
				String reason = response.getStatusLine().getReasonPhrase();

				// Workaround to handle response like this:
				// Bad request: { ... json here ... }
				if (entity.startsWith(reason)) {
					return mapper.readValue(entity.substring(entity.indexOf(":") + 1), AuthResponse.class);
				} else {
					return mapper.readValue(entity, AuthResponse.class);
				}
			}
		}
	}

	private String getProperty(String name, String defaultValue) {
		String value = config.getProperty(name, defaultValue);
		if (StringUtils.isEmpty(value))
			throw new IllegalArgumentException("Missing system property: " + name);
		return value;
	}
}
