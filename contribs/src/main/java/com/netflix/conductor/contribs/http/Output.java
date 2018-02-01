package com.netflix.conductor.contribs.http;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Output {
	private Validate validate;

	public Validate getValidate() {
		return validate;
	}

	public void setValidate(Validate validate) {
		this.validate = validate;
	}
}