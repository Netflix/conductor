package com.netflix.conductor.sqlserver.util;

import java.sql.SQLException;

/**
 * Functional interface for {@link Query} executions with no expected result.
 * @author mustafa
 */
@FunctionalInterface
public interface ExecuteFunction {
	void apply(Query query) throws SQLException;
}
