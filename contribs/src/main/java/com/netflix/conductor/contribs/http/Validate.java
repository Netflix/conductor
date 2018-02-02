package com.netflix.conductor.contribs.http;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Validate {
	private Map<String, String> conditions;
	private String reason;

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
	 * @return the reason
	 */
	public String getReason() {
		return reason;
	}

	/**
	 * @param reason the reason to set
	 */
	public void setReason(String reason) {
		this.reason = reason;
	}

}