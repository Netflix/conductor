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

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.conductor.common.run.CommonParams;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.contribs.correlation.Correlator;
import com.netflix.conductor.core.DNSLookup;
import com.netflix.conductor.core.config.Configuration;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Singleton
public class AuthManager {
	private static final Logger logger = LoggerFactory.getLogger(AuthManager.class);
	public static final String MISSING_PROPERTY = "Missing system property: ";
	public static final String PROPERTY_URL = "conductor.auth.url";
	public static final String PROPERTY_SERVICE = "conductor.auth.service";
	public static final String PROPERTY_ENDPOINT = "conductor.auth.endpoint";
	public static final String PROPERTY_CLIENT = "conductor.auth.clientId";
	public static final String PROPERTY_SECRET = "conductor.auth.clientSecret";
	private final ObjectMapper mapper = new ObjectMapper();
	private final String clientSecret;
	private final String clientId;
	private final String authUrl;
	private final String authService;
	private String authEndpoint;
	private boolean traceIdEnabled;

	@Inject
	public AuthManager(Configuration config) {
		authUrl = config.getProperty(PROPERTY_URL, null);
		if (StringUtils.isEmpty(authUrl))
			throw new IllegalArgumentException(MISSING_PROPERTY + PROPERTY_URL);

		authService = config.getProperty(PROPERTY_SERVICE, null);
		if (StringUtils.isNotEmpty(authService)) {
			authEndpoint = config.getProperty(PROPERTY_ENDPOINT, null);

			if (StringUtils.isEmpty(authEndpoint)) {
				throw new IllegalArgumentException(MISSING_PROPERTY + PROPERTY_ENDPOINT);
			}
		}

		clientId = config.getProperty(PROPERTY_CLIENT, null);
		if (StringUtils.isEmpty(clientId))
			throw new IllegalArgumentException(MISSING_PROPERTY + PROPERTY_CLIENT);

		clientSecret = config.getProperty(PROPERTY_SECRET, null);
		if (StringUtils.isEmpty(clientSecret))
			throw new IllegalArgumentException(MISSING_PROPERTY + PROPERTY_SECRET);

		traceIdEnabled = Boolean.parseBoolean(config.getProperty("workflow.traceid.enabled", "false"));
	}

	public AuthResponse authorize(Workflow workflow) throws Exception {
		Client client = Client.create();
		MultivaluedMap<String, String> data = new MultivaluedMapImpl();
		data.add("grant_type", "client_credentials");
		data.add("client_id", this.clientId);
		data.add("client_secret", this.clientSecret);

		String url = this.authUrl;
		String hostAndPort = DNSLookup.lookup(this.authService);
		if (StringUtils.isEmpty(hostAndPort)) {
			logger.error("Service lookup failed for " + this.authService + " falling back to: " + this.authUrl);
		} else {
			url = hostAndPort + authEndpoint;
		}

		WebResource.Builder webResource = client.resource(url).type(MediaType.APPLICATION_FORM_URLENCODED_TYPE);
		ClientResponse response;

		if (traceIdEnabled) {
			webResource.header(CommonParams.PLATFORM_TRACE_ID, workflow.getTraceId());
		}

		if (StringUtils.isNotEmpty(workflow.getCorrelationId())) {
			Correlator correlator = new Correlator(logger, workflow.getCorrelationId());
			correlator.updateSequenceNo();

			webResource.header(CommonParams.DELUXE_OWF_CONTEXT, correlator.asCorrelationId());
		}

		response = webResource.post(ClientResponse.class, data);

		if (response.getStatus() == 200) {
			String entity = response.getEntity(String.class);
			return mapper.readValue(entity, AuthResponse.class);
		} else {
			String entity = response.getEntity(String.class);
			if (StringUtils.isEmpty(entity)) {
				return new AuthResponse("no content", "server did not return body");
			}
			String reason = response.getStatusInfo().getReasonPhrase();

			// Workaround to handle response like this:
			// Bad request: { ... json here ... }
			if (entity.startsWith(reason)) {
				return mapper.readValue(entity.substring(entity.indexOf(":") + 1), AuthResponse.class);
			} else {
				return mapper.readValue(entity, AuthResponse.class);
			}
		}		
	}

	public Map<String, Object> validate(String token, Map<String, String> rules) {
		Map<String, Object> decoded = decode(token);
		return validate(decoded, rules);
	}

	public Map<String, Object> validate(Map<String, Object> decoded, Map<String, String> rules) {
		if (decoded.get("exp") == null) {
			throw new IllegalArgumentException("Invalid token. No expiration claim present");
		}
		long exp = Long.valueOf(decoded.get("exp").toString()) * 1000;
		if (System.currentTimeMillis() >= exp) {
			throw new IllegalArgumentException("Invalid token. Token is expired");
		}

		JsonNode input = mapper.valueToTree(decoded);

		LoadingCache<String, JsonQuery> queryCache = createQueryCache();

		Map<String, Object> failed = new HashMap<>();
		rules.forEach((rule, condition) -> {
			try {
				JsonQuery query = queryCache.get(condition);
				List<JsonNode> result = query.apply(input);
				if (result == null || result.isEmpty()) {
					logger.error("Verify failed for " + rule + " rule with no result!");
				} else {
					boolean success = Boolean.parseBoolean(result.iterator().next().toString());
					if (!success) {
						logger.debug("Verify failed for " + rule + " rule");
						failed.put(rule, false);
					}
				}
			} catch (Exception ex) {
				logger.error("Verify failed for " + rule + " with " + ex.getMessage(), ex);
				failed.put(rule, ex.getMessage());
			}
		});

		return failed;
	}

	public Map<String, Object> decode(String token) {
		try {
			DecodedJWT decodedJWT = JWT.decode(token);
			String decodedPayload = new String(Base64.decodeBase64(decodedJWT.getPayload()), StandardCharsets.UTF_8);

			return mapper.readValue(decodedPayload, new TypeReference<HashMap<String, Object>>() {});
		} catch (Exception ex) {
			throw new RuntimeException("Token decode failed: " + ex.getMessage(), ex);
		}
	}

	private LoadingCache<String, JsonQuery> createQueryCache() {
		CacheLoader<String, JsonQuery> loader = new CacheLoader<String, JsonQuery>() {
			@Override
			public JsonQuery load(@Nonnull String query) throws JsonQueryException {
				return JsonQuery.compile(query);
			}
		};
		return CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).maximumSize(1000).build(loader);
	}
}
