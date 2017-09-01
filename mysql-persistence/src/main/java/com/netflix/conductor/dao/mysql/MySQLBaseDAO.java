package com.netflix.conductor.dao.mysql;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

abstract class MySQLBaseDAO {

	private static final List<String> EXCLUDED_STACKTRACE_CLASS = ImmutableList.of("com.netflix.conductor.dao.mysql.MySQLBaseDAO", "java.lang.Thread");

	protected final Sql2o sql2o;
	protected final ObjectMapper om;
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected MySQLBaseDAO(ObjectMapper om, Sql2o sql2o) {
		this.om = om;
		this.sql2o = sql2o;
	}

	protected String toJson(Object value) {
		try {
			return om.writeValueAsString(value);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	protected <T>T readValue(String json, Class<T> clazz) {
		try {
			return om.readValue(json, clazz);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected <R> R getWithTransaction(Function<Connection, R> function) {
		Instant start = Instant.now();
		StackTraceElement caller = Arrays.stream(Thread.currentThread().getStackTrace()).filter(ste -> !EXCLUDED_STACKTRACE_CLASS.contains(ste.getClassName())).findFirst().get();
		logger.debug("{} : starting transaction", caller.getMethodName());
		try (Connection connection = sql2o.beginTransaction(TRANSACTION_READ_COMMITTED)) {
			final R result = function.apply(connection);
			connection.commit();
			return result;
		} finally {
			Instant end = Instant.now();
			logger.debug("{} : took {}ms", caller.getMethodName(), Duration.between(start, end).toMillis());
		}
	}

	protected void withTransaction(Consumer<Connection> consumer) {
		getWithTransaction(connection -> {
			consumer.accept(connection);
			return null;
		});
	}
}
