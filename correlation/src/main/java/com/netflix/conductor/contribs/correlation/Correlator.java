package com.netflix.conductor.contribs.correlation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;

import javax.ws.rs.core.HttpHeaders;
import java.util.List;
import java.util.Map;

/**
 * Created by beimforz on 12/21/17.
 */
public class Correlator implements ICorrelator {
	public static final String headerKey = "Deluxe-Owf-Context";
	private static final ObjectMapper mapper = new ObjectMapper();
	private final Logger logger;
	private Context context;

	private Correlator(Logger logger) {
		this.logger = logger;
	}

	public Correlator(Logger logger, String correlationId) {
		this(logger);
		this.context = parseCorrelationId(correlationId);
		logger.debug("Context from correlationId is " + context);
	}

	public Correlator(Logger logger, HttpHeaders headers) {
		this(logger);
		this.context = parseHeader(headers);
		logger.debug("Context from headers is " + context);
	}

	public String asCorrelationId() throws JsonProcessingException {
		return mapper.writeValueAsString(context);
	}

	public void addIdentifier(String urn) {
		if (urn == null) {
			return;
		}
		urn = urn.trim().toLowerCase();
		if (context == null) {
			return;
		}
		if (context.getUrns().contains(urn)) {
			return;
		}
		context.getUrns().add(urn);
		logger.debug("Context after urn set is " + context);
	}


	public void attach(Map<String, Object> headers) throws JsonProcessingException {
		String json = mapper.writeValueAsString(context);
		headers.put(headerKey, json);
	}

	private Context parseCorrelationId(String value) {
		try {
			return mapper.readValue(value, Context.class);
		} catch (Exception e) {
			logger.error("Unable to parse " + value + " from correlationId", e);
		}
		return new Context();
	}

	private Context parseHeader(HttpHeaders headers) {
		List<String> values = headers.getRequestHeader(headerKey);
		if (values != null && !values.isEmpty()) {
			String value = values.get(0);
			try {
				String json = StringEscapeUtils.unescapeJson(value);
				return mapper.readValue(json, Context.class);
			} catch (Exception e) {
				logger.error("Unable to parse " + value + " from " + headerKey + " header", e);
			}
		}

		return new Context();
	}

	public void updateSequenceNo() {
		int sequenceNo = context.getSequenceno();
		sequenceNo++;
		context.setSequenceno(sequenceNo);
	}

	public Context getContext() {
		return context;
	}
}
