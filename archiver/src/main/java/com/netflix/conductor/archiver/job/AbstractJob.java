package com.netflix.conductor.archiver.job;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractJob {
    final String DELETE = "DELETE FROM %s WHERE id = ANY(?)";
    HikariDataSource dataSource;

    AbstractJob(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    List<Integer> fetchIds(String query, Timestamp endTime, int limit) throws SQLException {
        LinkedList<Integer> result = new LinkedList<>();
        try (Connection tx = dataSource.getConnection(); PreparedStatement st = tx.prepareStatement(query)) {
            st.setTimestamp(1, endTime);
            st.setInt(2, limit);

            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    result.add(rs.getInt("id"));
                }
            }
        }
        return result;
    }

    List<Integer> fetchIds(String query, List<String> cleanupWorkflows, int limit) throws SQLException {
        LinkedList<Integer> result = new LinkedList<>();
        try (Connection tx = dataSource.getConnection(); PreparedStatement st = tx.prepareStatement(query)) {
            String[] values = cleanupWorkflows.toArray(new String[0]);
            Array arrayOfWorkflows = tx.createArrayOf("text", values);

            st.setArray(1, arrayOfWorkflows);
            st.setInt(2, limit);

            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    result.add(rs.getInt("id"));
                }
            }
        }
        return result;
    }

    int deleteByIds(String table, List<Integer> ids) throws SQLException {
        String query = String.format(DELETE, table);

        try (Connection tx = dataSource.getConnection(); PreparedStatement st = tx.prepareStatement(query)) {
            Integer[] values = ids.toArray(new Integer[0]);

            Array arrayOf = tx.createArrayOf("bigint", values);
            st.setArray(1, arrayOf);

            return st.executeUpdate();
        }
    }

    public abstract void cleanup() throws Exception;
}
