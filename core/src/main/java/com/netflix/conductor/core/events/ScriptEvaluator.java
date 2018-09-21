/**
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.conductor.core.events;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nonnull;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Viren
 *
 */
public class ScriptEvaluator {

	private static ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
	private static LoadingCache<String, JsonQuery> queryCache = createQueryCache();
	private static final ObjectMapper om = new ObjectMapper();

	private ScriptEvaluator() {

	}

	public static Boolean evalBool(String script, Object input) throws ScriptException {
		Object ret = eval(script, input);

		if (ret instanceof Boolean) {
			return ((Boolean) ret);
		} else if (ret instanceof Number) {
			return ((Number) ret).doubleValue() > 0;
		}
		return false;
	}

	public static Object eval(String script, Object input) throws ScriptException {
		Bindings bindings = engine.createBindings();
		bindings.put("$", input);
		return engine.eval(script, bindings);

	}

	public static String evalJq(String expression, Object payload) throws Exception {
		JsonNode input = om.valueToTree(payload);
		JsonQuery query = queryCache.get(expression);
		List<JsonNode> result = query.apply(input);
		if (result == null || result.isEmpty()) {
			return null;
		} else {
			return result.get(0).asText(null);
		}
	}

	public static List<Object> evalJqAsList(String expression, Object payload) throws Exception {
		JsonNode input = om.valueToTree(payload);
		JsonQuery query = queryCache.get(expression);
		List<JsonNode> result = query.apply(input);
		if (result == null || result.isEmpty()) {
			return Collections.emptyList();
		} else {
			return om.convertValue(result, new TypeReference<List<Object>>(){});
		}
	}

	public static Map<String, String> evaluateMap(Map<String, String> map, Object payload) {
		return map.entrySet().stream().map(entry -> {
			String fieldName = entry.getKey();
			String expression = entry.getValue();
			if (StringUtils.isEmpty(expression))
				throw new RuntimeException(fieldName + " expression is empty");

			String fieldValue;
			try {
				fieldValue = evalJq(expression, payload);
			} catch (Exception e) {
				throw new RuntimeException(fieldName + " evaluating failed with " + e.getMessage(), e);
			}

			return new HashMap.SimpleEntry<>(fieldName, fieldValue);
		}).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
	}

	private static LoadingCache<String, JsonQuery> createQueryCache() {
		CacheLoader<String, JsonQuery> loader = new CacheLoader<String, JsonQuery>() {
			public JsonQuery load(@Nonnull String query) throws JsonQueryException {
				return JsonQuery.compile(query);
			}
		};
		return CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).maximumSize(1000).build(loader);
	}
}
