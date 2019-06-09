package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.netflix.conductor.aurora.sql.*;
import com.netflix.conductor.core.execution.ApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

abstract class AuroraBaseDAO {
	private static final List<String> EXCLUDED_STACKTRACE_CLASS = ImmutableList.of(
		AuroraBaseDAO.class.getName(),
		Thread.class.getName()
	);

	protected final Logger logger = LoggerFactory.getLogger(getClass());
	private final DataSource dataSource;
	private final ObjectMapper mapper;

	AuroraBaseDAO(DataSource dataSource, ObjectMapper mapper) {
		this.dataSource = dataSource;
		this.mapper = mapper;
	}

	void withTransaction(Consumer<Connection> consumer) {
		getWithTransaction(connection -> {
			consumer.accept(connection);
			return null;
		});
	}

	<R> R getWithTransaction(TransactionalFunction<R> function) {
		Instant start = Instant.now();
		LazyToString callingMethod = getCallingMethod();
		if (logger.isTraceEnabled())
			logger.trace("{} : starting transaction", callingMethod);

		try (Connection tx = dataSource.getConnection()) {
			boolean previousAutoCommitMode = tx.getAutoCommit();
			tx.setAutoCommit(false);
			try {
				R result = function.apply(tx);
				tx.commit();
				return result;
			} catch (Throwable th) {
				tx.rollback();
				throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, th.getMessage(), th);
			} finally {
				tx.setAutoCommit(previousAutoCommitMode);
			}
		} catch (SQLException ex) {
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex.getMessage(), ex);
		} finally {
			if (logger.isTraceEnabled())
				logger.trace("{} : took {}ms", callingMethod, Duration.between(start, Instant.now()).toMillis());
		}
	}

	private LazyToString getCallingMethod() {
		return new LazyToString(() -> Arrays.stream(Thread.currentThread().getStackTrace())
			.filter(ste -> !EXCLUDED_STACKTRACE_CLASS.contains(ste.getClassName()))
			.findFirst()
			.map(StackTraceElement::getMethodName)
			.orElseThrow(() -> new NullPointerException("Cannot find Caller")));
	}

	<R> R query(Connection tx, String query, QueryFunction<R> function) {
		if (logger.isTraceEnabled()) {
			LazyToString callingMethod = getCallingMethod();
			logger.trace("{} : executing {}", callingMethod, query);
		}
		try (Query q = new Query(mapper, tx, query)) {
			return function.apply(q);
		} catch (SQLException ex) {
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex);
		}
	}

	void execute(Connection tx, String query, ExecuteFunction function) {
		if (logger.isTraceEnabled()) {
			LazyToString callingMethod = getCallingMethod();
			logger.trace("{} : executing {}", callingMethod, query);
		}
		try (Query q = new Query(mapper, tx, query)) {
			function.apply(q);
		} catch (SQLException ex) {
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex);
		}
	}

	<R> R queryWithTransaction(String query, QueryFunction<R> function) {
		return getWithTransaction(tx -> query(tx, query, function));
	}

	void executeWithTransaction(String query, ExecuteFunction function) {
		withTransaction(tx -> execute(tx, query, function));
	}

	<T> T readValue(String json, Class<T> tClass) {
		try {
			return mapper.readValue(json, tClass);
		} catch (IOException ex) {
			throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, ex);
		}
	}
}
