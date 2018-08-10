package com.netflix.conductor.contribs.http;

import javax.ws.rs.core.MultivaluedMap;
import java.util.HashMap;
import java.util.Map;

public class HttpResponse {
	public Object body;

	public MultivaluedMap<String, String> headers;

	public int statusCode;

	public String error;

	@Override
	public String toString() {
		return "HttpResponse [body=" + body + ", headers=" + headers + ", statusCode=" + statusCode + "]";
	}

	public Map<String, Object> asMap() {

		Map<String, Object> map = new HashMap<>();
		map.put("body", body);
		map.put("error", error);
		map.put("headers", headers);
		map.put("statusCode", statusCode);

		return map;
	}
}
