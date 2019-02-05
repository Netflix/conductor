package com.netflix.conductor.tests.utils;

import java.io.InputStream;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtils {

	public static <T> T fromJson(String fileName, Class<T> classObject) throws Exception {

		ObjectMapper objectMapper = new ObjectMapper();

		InputStream inputStream = ClassLoader.getSystemResourceAsStream(fileName);
		return objectMapper.readValue(inputStream, classObject);

	}
}