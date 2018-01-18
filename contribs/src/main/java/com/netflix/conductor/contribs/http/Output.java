package com.netflix.conductor.contribs.http;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Output {
	private Map<String, String> conditions;
	private String reasonParameter;

	/**
	 * @return the conditions map
	 */
	public Map<String, String> getConditions() {
		return conditions;
	}

	/**
	 * @param conditions the method to set
	 */
	public void setConditions(Map<String, String> conditions) {
		this.conditions = conditions;
	}


	/**
	 * @return the reasonParameter
	 */
	public String getReasonParameter() {
		return reasonParameter;
	}

	/**
	 * @param reasonParameter the reasonParameter to set
	 */
	public void setReasonParameter(String reasonParameter) {
		this.reasonParameter = reasonParameter;
	}

}