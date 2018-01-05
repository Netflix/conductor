package com.netflix.conductor.contribs.http;

import javax.ws.rs.core.MultivaluedMap;
import java.util.HashMap;
import java.util.Map;

public class HttpResponse {
	
	public Object body;
	
	public MultivaluedMap<String, String> headers;
	
	public int statusCode;
	
	public String reasonPhrase;
	
	@Override
	public String toString() {
		return "HttpResponse [body=" + body + ", headers=" + headers + ", statusCode=" + statusCode + ", reasonPhrase=" + reasonPhrase + "]";
	}
	
	public Map<String, Object> asMap() {
		
		Map<String, Object> map = new HashMap<>();
		map.put("body", body);
		map.put("headers", headers);
		map.put("statusCode", statusCode);
		map.put("reasonPhrase", reasonPhrase);
		
		return map;
	}
}
