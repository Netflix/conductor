package com.netflix.conductor.sqlserver.sql;

import com.netflix.conductor.dao.sqlserver.Query;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Functional interface for {@link Query#executeAndFetch(ResultSetHandler)}.
 * @author mustafa
 */
@FunctionalInterface
public interface ResultSetHandler<R> {
	R apply(ResultSet resultSet) throws SQLException;
}