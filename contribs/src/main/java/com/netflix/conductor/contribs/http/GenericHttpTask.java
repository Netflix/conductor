package com.netflix.conductor.contribs.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.DNSLookup;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.oauth.client.OAuthClientFilter;
import com.sun.jersey.oauth.signature.OAuthParameters;
import com.sun.jersey.oauth.signature.OAuthSecrets;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class GenericHttpTask extends WorkflowSystemTask {
	private static final Logger logger = LoggerFactory.getLogger(HttpTask.class);
	private static final String DELUXE_OWF_CORRELATION = "Deluxe-Owf-Correlation";
	private static final String SEQUENCE_NO = "sequence-no";

	protected RestClientManager rcm;
	protected Configuration config;
	protected ObjectMapper om;

	private TypeReference<Map<String, Object>> mapOfObj = new TypeReference<Map<String, Object>>() {
	};

	private TypeReference<List<Object>> listOfObj = new TypeReference<List<Object>>() {
	};

	GenericHttpTask(String name, Configuration config, RestClientManager rcm, ObjectMapper om) {
		super(name);
		this.config = config;
		this.rcm = rcm;
		this.om = om;
	}

	String lookup(String service) {
		DNSLookup lookup = new DNSLookup();
		DNSLookup.DNSResponses responses = lookup.lookupService(service);
		if (responses != null && ArrayUtils.isNotEmpty(responses.getResponses())) {
			String address = responses.getResponses()[0].getAddress();
			int port = responses.getResponses()[0].getPort();
			return "http://" + address + ":" + port;
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	HttpResponse httpCallUrlEncoded(Input input, String body) throws Exception {
		Client client = Client.create();
		MultivaluedMap formData = new MultivaluedMapImpl();
		Map<String, String> bodyparam = new ObjectMapper().readValue(body, HashMap.class);
		Iterator it = bodyparam.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			formData.add(pair.getKey(), pair.getValue());
			it.remove();
		}
		WebResource webResource = client.resource(input.getUri());

		ClientResponse response = webResource
				.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
				.post(ClientResponse.class, formData);

		if (response.getStatus() != 201 && response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus() + response.getEntity(String.class));


		}
		HttpResponse responsehttp = new HttpResponse();

		responsehttp.body = extractBody(response);
		responsehttp.statusCode = response.getStatus();
		responsehttp.headers = response.getHeaders();
		return responsehttp;

	}

	/**
	 * @param input HTTP Request
	 * @return Response of the http call
	 * @throws Exception If there was an error making http call
	 */
	HttpResponse httpCall(Input input, Workflow workflow, WorkflowExecutor executor) throws Exception {
		Client client = rcm.getClient(input);

		if (input.getOauthConsumerKey() != null) {
			logger.info("Configuring OAuth filter");
			OAuthParameters params = new OAuthParameters().consumerKey(input.getOauthConsumerKey()).signatureMethod("HMAC-SHA1").version("1.0");
			OAuthSecrets secrets = new OAuthSecrets().consumerSecret(input.getOauthConsumerSecret());
			client.addFilter(new OAuthClientFilter(client.getProviders(), params, secrets));
		}

		WebResource.Builder builder = client.resource(input.getUri()).type(MediaType.APPLICATION_JSON);

		if (input.getBody() != null) {
			builder.entity(input.getBody());
		}

		input.getHeaders().entrySet().forEach(e -> {
			builder.header(e.getKey(), e.getValue());
		});

		// Attach Deluxe Owf Correlation header and update workflow to save new sequence
		if (input.isCorrelation()) {
			setCorrelation(builder, workflow);
			executor.updateWorkflow(workflow);
		}

		HttpResponse response = new HttpResponse();
		try {
			ClientResponse cr = builder.accept(input.getAccept()).method(input.getMethod(), ClientResponse.class);
			if (cr.getStatus() != 204 && cr.hasEntity()) {
				response.body = extractBody(cr);
			}
			response.statusCode = cr.getStatus();
			response.headers = cr.getHeaders();
			return response;
		} catch (UniformInterfaceException ex) {
			logger.error(ex.getMessage(), ex);
			ClientResponse cr = ex.getResponse();
			logger.error("Status Code: {}", cr.getStatus());
			if (cr.getStatus() > 199 && cr.getStatus() < 300) {
				if (cr.getStatus() != 204 && cr.hasEntity()) {
					response.body = extractBody(cr);
				}
				response.headers = cr.getHeaders();
				response.statusCode = cr.getStatus();
				return response;
			} else {
				String reason = cr.getEntity(String.class);
				logger.error(reason, ex);
				throw new Exception(reason);
			}
		}

	}

	private Object extractBody(ClientResponse cr) {
		String json = cr.getEntity(String.class);
		try {
			JsonNode node = om.readTree(json);
			if (node.isArray()) {
				return om.convertValue(node, listOfObj);
			} else if (node.isObject()) {
				return om.convertValue(node, mapOfObj);
			} else if (node.isNumber()) {
				return om.convertValue(node, Double.class);
			} else {
				return node.asText();
			}

		} catch (IOException jpe) {
			logger.error(jpe.getMessage(), jpe);
			return json;
		}
	}

	@SuppressWarnings("unchecked")
	private void setCorrelation(WebResource.Builder builder, Workflow workflow) throws JsonProcessingException {
		// Exit if no headers at all
		if (workflow.getHeaders() == null) {
			return;
		}

		// Exit if no required header
		if (!workflow.getHeaders().containsKey(DELUXE_OWF_CORRELATION)) {
			return;
		}

		Map<String, Object> header = (Map<String, Object>)workflow.getHeaders().get(DELUXE_OWF_CORRELATION);
		int sequence = (int)header.get(SEQUENCE_NO);
		header.put(SEQUENCE_NO, sequence + 1);

		String json = om.writeValueAsString(header);
		logger.info("Setting " + DELUXE_OWF_CORRELATION + " header with " + json);

		// Set the header with json value
		builder.header(DELUXE_OWF_CORRELATION, json);
	}
}
