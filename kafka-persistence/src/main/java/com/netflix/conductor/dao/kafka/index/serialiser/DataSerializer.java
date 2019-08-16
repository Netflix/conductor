package com.netflix.conductor.dao.kafka.index.serialiser;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class DataSerializer extends StdSerializer<Record> {

	public DataSerializer() {
		this(null);
	}

	public DataSerializer(Class<Record> t) {
		super(t);
	}

	@Override
	public void serialize(
            Record value, JsonGenerator jgen, SerializerProvider provider)
			throws IOException {

		jgen.writeStartObject();
		jgen.writeStringField("type", value.type);
		jgen.writeObjectField("payload", value.payload);
		jgen.writeEndObject();
	}

}
