package com.netflix.conductor.sqlserver.sql;

import com.netflix.conductor.dao.sqlserver.Query;

import java.sql.SQLException;

/**
 * Functional interface for {@link Query} executions that return results.
 * @author mustafa
 */
@FunctionalInterface
public interface QueryFunction<R> {
	R apply(Query query) throws SQLException;
}
