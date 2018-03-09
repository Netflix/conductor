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
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.conductor.core.config.Configuration;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Singleton
public class AuthManager {
	private static final Logger logger = LoggerFactory.getLogger(AuthManager.class);
	public static final String MISSING_PROPERTY = "Missing system property: ";
	public static final String PROPERTY_URL = "conductor.auth.url";
	public static final String PROPERTY_CLIENT = "conductor.auth.clientId";
	public static final String PROPERTY_SECRET = "conductor.auth.clientSecret";
	private final ObjectMapper mapper = new ObjectMapper();
	private final String clientSecret;
	private final String clientId;
	private final String authUrl;

	@Inject
	public AuthManager(Configuration config) {
		authUrl = config.getProperty(PROPERTY_URL, null);
		if (StringUtils.isEmpty(authUrl))
			throw new IllegalArgumentException(MISSING_PROPERTY + PROPERTY_URL);

		clientId = config.getProperty(PROPERTY_CLIENT, null);
		if (StringUtils.isEmpty(clientId))
			throw new IllegalArgumentException(MISSING_PROPERTY + PROPERTY_CLIENT);

		clientSecret = config.getProperty(PROPERTY_SECRET, null);
		if (StringUtils.isEmpty(clientSecret))
			throw new IllegalArgumentException(MISSING_PROPERTY + PROPERTY_SECRET);
	}

	public AuthResponse authorize() throws Exception {
		Client client = Client.create();
		MultivaluedMap<String, String> data = new MultivaluedMapImpl();
		data.add("grant_type", "client_credentials");
		data.add("client_id", this.clientId);
		data.add("client_secret", this.clientSecret);

		WebResource webResource = client.resource(this.authUrl);

		ClientResponse response = webResource
				.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
				.post(ClientResponse.class, data);

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
		Map<String, Object> payload = decode(token);
		Long exp = (Long)payload.get("exp");
		if (exp == null) {
			throw new IllegalArgumentException("Invalid token. No expiration claim present");
		}
		exp *= 1000; // Convert to milliseconds
		if (System.currentTimeMillis() >= exp) {
			throw new IllegalArgumentException("Invalid token. Token is expired");
		}

		JsonNode input = mapper.valueToTree(payload);

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
						logger.info("Verify failed for " + rule + " rule");
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
		DecodedJWT decoded = JWT.decode(token);
		Map<String, Claim> claims = decoded.getClaims();

		Map<String, Object> payload = new HashMap<>();
		claims.forEach((key, value) -> {
			if (value.isNull()) {
				payload.put(key, null);
			} else if (value.asMap() != null) {
				payload.put(key, value.asMap());
			} else if (value.asLong() != null) {
				payload.put(key, value.asLong());
			} else if (value.asBoolean() != null) {
				payload.put(key, value.asBoolean());
			} else if (value.asDate() != null) {
				payload.put(key, value.asDate());
			} else if (value.asDouble() != null) {
				payload.put(key, value.asDouble());
			} else if (value.asInt() != null) {
				payload.put(key, value.asInt());
			} else {
				payload.put(key, value.asString());
			}
		});

		return payload;
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

	/*
	public static void main(String[] args) {
		Map<String, Object> claims = decode(token);
		System.out.println("claims = " + claims);
		Long exp = (Long)claims.get("exp");
		if (exp == null) {
			throw new RuntimeException("Invalid token. No expiration claim present");
		}
		exp *= 1000; // Convert to milliseconds

		boolean expired = System.currentTimeMillis() >= exp;
		System.out.println("expired = " + expired + ", date is " + new Date(exp));
	}
	*/

	//static String token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIzUmVQUmxxaDBSdUxfcUd5c3dSZXRJbjZNZkZWblp4S2NCdU5GaW55QlM4In0.eyJqdGkiOiI0MjRiNmU4MC05MzU1LTRlNzItOGExOC01NTJjNDc2OTQzMGMiLCJleHAiOjE1MjA1ODc2MjUsIm5iZiI6MCwiaWF0IjoxNTIwNTg2NzI1LCJpc3MiOiJodHRwczovL2tleWNsb2FrLmRtbGliLmRlL2F1dGgvcmVhbG1zL2RlbHV4ZSIsImF1ZCI6ImRlbHV4ZS5jb25kdWN0b3IiLCJzdWIiOiJmN2EzNGRjZi1lYjg5LTQwYTgtYWY2Ni1kZTQ4ODFjNTI4OWEiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJkZWx1eGUuY29uZHVjdG9yIiwiYXV0aF90aW1lIjowLCJzZXNzaW9uX3N0YXRlIjoiYzhhZjM1MDItYWIzNC00NTA3LThlNDItZTc2YjI3M2IxYWNkIiwiYWNyIjoiMSIsImNsaWVudF9zZXNzaW9uIjoiMmRiZjk2MGEtNzY4Ni00MjU5LTkxNjMtY2QwZDgzMmYzMjM5IiwiYWxsb3dlZC1vcmlnaW5zIjpbXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImF1dGgtdXNlciIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiZGVsdXhlLmF0bGFzIjp7InJvbGVzIjpbImRlbHV4ZS5hdGxhcy5hc3NldC13cml0ZSIsImRlbHV4ZS5hdGxhcy5hc3NldC1yZWFkIl19LCJkZWx1eGUuY29uZHVjdG9yIjp7InJvbGVzIjpbImRlbHV4ZS5jb25kdWN0b3IuYWRtaW4iXX0sImRlbHV4ZS5hdXRoIjp7InJvbGVzIjpbImRlbHV4ZS5hdXRoLmFkbWluIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJjbGllbnRIb3N0IjoiNTQuMjEzLjQxLjQ5IiwiY2xpZW50SWQiOiJkZWx1eGUuY29uZHVjdG9yIiwibmFtZSI6IiIsInByZWZlcnJlZF91c2VybmFtZSI6InNlcnZpY2UtYWNjb3VudC1kZWx1eGUuY29uZHVjdG9yIiwiY2xpZW50QWRkcmVzcyI6IjU0LjIxMy40MS40OSIsImVtYWlsIjoic2VydmljZS1hY2NvdW50LWRlbHV4ZS5jb25kdWN0b3JAcGxhY2Vob2xkZXIub3JnIn0.1G-wvD75-ak60zW6KJgz7kUj_zWbnpZpWXS5b-UWnRVlpPyVYoJhAjxHPTTIBFSl5yRFtg3ZwBisVahtWQcU0fz1EZY4sFMn5rAOy_8rpWptZjnMIhXdk8bSPF1KiVV_I59Ha4MZWjO8bA_yzmJ9vMlE3RoI9teAl4Rvhxp_OUyBqfA3PX6ob716BBIRJqnmrEGtqMjpJ90XlTZfFWtUwiVkNcHlES2SXGuiQIAi6Be0vslIgY55s8CPO_HhZUtvCRY1lnGm5ifrAHX__gISwxczffTxT1kov-LO8rJWqCn57-B1YJTtZeyEunUmVTks2rFcGWFzolM0AuHBlUaCeg";
	static String token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIzUmVQUmxxaDBSdUxfcUd5c3dSZXRJbjZNZkZWblp4S2NCdU5GaW55QlM4In0.eyJqdGkiOiJkMjhmMjEyMC02YmEwLTQyMWEtOWEzYi0xMjAwZDVkOTc1ODEiLCJleHAiOjE1MjA2MDA0OTEsIm5iZiI6MCwiaWF0IjoxNTIwNTk5NTkxLCJpc3MiOiJodHRwczovL2tleWNsb2FrLmRtbGliLmRlL2F1dGgvcmVhbG1zL2RlbHV4ZSIsImF1ZCI6ImRlbHV4ZS5jb25kdWN0b3IiLCJzdWIiOiJmN2EzNGRjZi1lYjg5LTQwYTgtYWY2Ni1kZTQ4ODFjNTI4OWEiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJkZWx1eGUuY29uZHVjdG9yIiwiYXV0aF90aW1lIjowLCJzZXNzaW9uX3N0YXRlIjoiMGJhODFlMTUtZWFlOS00MzBhLTk3NDQtOTU1MTc0ZDBkYjcyIiwiYWNyIjoiMSIsImNsaWVudF9zZXNzaW9uIjoiY2E1YWZiY2ItM2YxNC00N2RiLTk3MjQtMzdkZWJmZWY1NjJkIiwiYWxsb3dlZC1vcmlnaW5zIjpbXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImF1dGgtdXNlciIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiZGVsdXhlLmF0bGFzIjp7InJvbGVzIjpbImRlbHV4ZS5hdGxhcy5hc3NldC13cml0ZSIsImRlbHV4ZS5hdGxhcy5hc3NldC1yZWFkIl19LCJkZWx1eGUuY29uZHVjdG9yIjp7InJvbGVzIjpbImRlbHV4ZS5jb25kdWN0b3IuYWRtaW4iXX0sImRlbHV4ZS5hdXRoIjp7InJvbGVzIjpbImRlbHV4ZS5hdXRoLmFkbWluIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJjbGllbnRIb3N0IjoiMzQuMjE2LjczLjY2IiwiY2xpZW50SWQiOiJkZWx1eGUuY29uZHVjdG9yIiwibmFtZSI6IiIsInByZWZlcnJlZF91c2VybmFtZSI6InNlcnZpY2UtYWNjb3VudC1kZWx1eGUuY29uZHVjdG9yIiwiY2xpZW50QWRkcmVzcyI6IjM0LjIxNi43My42NiIsImVtYWlsIjoic2VydmljZS1hY2NvdW50LWRlbHV4ZS5jb25kdWN0b3JAcGxhY2Vob2xkZXIub3JnIn0.pCo1sRbXfkkkh5DvepZhA0Zl4KUVCSxHGOqsRCNX7CoXLbKXA-MUwoBevgJKzgDsCoygVAAiv45RKeNdFwLcmThy6fj5UIFLJM45KR5k5AFl8vC4aV1YZChqELNqE3Qjl4J9B0LC-ERlufoFeuYPoMs9EmlX8LrRcyD35Rreok1eCBBaj7gSsQHHoUJUXhF5In9G2s330oGaThuEDB9QU4eTpcxV4n4oqHUn8E5POialutalEfwUxBOPnPYYUvt3f07_1GSNZomSAO68jCJjLphzsQWjgB54CjB4TdE2kbLdCGwfepbMOESI1Vk7SPGsh7DJAY7SnmypQVi89bLECQ";
}
