package com.netflix.conductor.dao.kafka.index.serialiser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

public class DataDeSerializer extends StdDeserializer<Record> {

	public DataDeSerializer() {
		this(null);
	}

	@Override
	public Record deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
		JsonNode node = jp.getCodec().readTree(jp);
		String type = node.get("type").asText();
		Object payload = node.get("payload");

		return new Record(type, payload);
	}

	public DataDeSerializer(Class<Record> t) {
		super(t);
	}
}
