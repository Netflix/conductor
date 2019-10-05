package com.netflix.conductor.sql;

import com.netflix.conductor.dao.sql.Query;

import java.sql.SQLException;

/**
 * Functional interface for {@link Query} executions with no expected result.
 * @author mustafa
 */
@FunctionalInterface
public interface ExecuteFunction {
	void apply(Query query) throws SQLException;
}
