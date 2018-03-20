package com.netflix.conductor.contribs.correlation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import javax.ws.rs.core.HttpHeaders;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by beimforz on 12/21/17.
 */
public class Correlator implements ICorrelator {
	public final static String headerKey = "Deluxe-Owf-Context";

	private ObjectMapper mapper = new ObjectMapper();
	private Context context;
	private Logger logger;

	public Correlator(Logger logger, Context context) {
		this.logger = logger;
		this.context = context;
	}

	public Correlator(Logger logger, HttpHeaders headers) {
		this.logger = logger;
		this.context = parseHeader(headers);

		logger.info("Initial context is " + context.print());
	}

	public Correlator(Logger logger, Map<String, Object> context) {
		this.logger = logger;
		this.context = mapper.convertValue(context, Context.class);
	}

	public Map<String, Object> getAsMap() {
		return mapper.convertValue(context, new TypeReference<Map<String, Object>>() {
		});
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
		List<String> urns = context.getUrns();
		urns.add(urn);
		context.setUrns(urns);

		logger.info("Context after urn set is " + context.print());
	}

	public void attach(Map<String, Object> headers) throws JsonProcessingException {
		String json = mapper.writeValueAsString(context);
		headers.put(headerKey, json);
	}

	public Context parseHeader(HttpHeaders headers) {
		Context result = new Context();
		ArrayList<Context> contexts = new ArrayList<>();
		Set<String> keys = headers.getRequestHeaders().keySet();
		if (headers.getRequestHeader(headerKey) != null) {
			for (String key : keys) {
				if (key.equalsIgnoreCase(headerKey)) {
					try {
						String value = headers.getRequestHeaders().get(key).get(0);
						Context rawContext = mapper.readValue(value, Context.class);
						contexts.add(rawContext);
					} catch (Exception e) {
						logger.error("Unable to parse " + headerKey + " header", e);
					}
				}
			}
			result = merge(contexts);
		}

		List<String> newValues = result.getUrns().stream().map(String::toLowerCase).distinct().collect(Collectors.toList());
		result.setUrns(newValues);
		return result;
	}

	public Context merge(ArrayList<Context> contexts) {
		Context result = new Context();
		for (Context context : contexts) {
			result.setSequenceno(Math.max(result.getSequenceno(), context.getSequenceno()));
			List<String> mergedList = result.getUrns();
			mergedList.addAll(context.getUrns());
			result.setUrns(mergedList.stream().distinct().collect(Collectors.toList()));
		}
		return result;
	}

	public Context updateSequenceNo() {
		int sequenceNo = context.getSequenceno();
		sequenceNo++;
		context.setSequenceno(sequenceNo);
		return context;
	}
}
