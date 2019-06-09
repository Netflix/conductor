/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.aurora;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * @author Oleksiy Lysak
 *
 */
public class AuroraModule extends AbstractModule {
	private static Logger logger = LoggerFactory.getLogger(AuroraModule.class);

	@Override
	protected void configure() {
        bind(DataSource.class).toProvider(AuroraDataSourceProvider.class).in(Scopes.SINGLETON);
		logger.debug("Aurora Module configured ...");
	}

	public static void main(String[] args) throws SQLException {
		String url = "jdbc:postgresql://192.168.1.100:5432/postgres";

		HikariConfig poolConfig = new HikariConfig();
		poolConfig.setJdbcUrl(url);
		poolConfig.setUsername("postgres");
		poolConfig.setPassword("postgres");
		poolConfig.setAutoCommit(false);

		HikariDataSource dataSource = new HikariDataSource(poolConfig);

		AuroraModule module = new AuroraModule();
		module.testSet(dataSource);
	}

	private void testSet(HikariDataSource dataSource) throws SQLException {
		Set<String> tags = new HashSet<>(Arrays.asList("t1", "t2", "t4"));
		String[] values = tags.toArray(new String[0]);

		String SQL = "select true from workflow where tags = ? limit 1";

		Connection connection = dataSource.getConnection();
		PreparedStatement ptmt = connection.prepareStatement(SQL);
		ptmt.setArray(1, connection.createArrayOf("text", values));
		ResultSet set = ptmt.executeQuery();
		while (set.next()) {
			boolean id = set.getBoolean(1);
			System.out.println("id = " + id);
		}
		connection.commit();
	}

	private void bulkInsert(HikariDataSource dataSource) throws SQLException {
		String SQL = "insert into workflow (workflow_id, json_data, tags) values (?, ?, ?)";
		Connection connection = dataSource.getConnection();
		connection.setAutoCommit(true);
		PreparedStatement ptmt = connection.prepareStatement(SQL);

		String[] values = new String[] {UUID.randomUUID().toString(), UUID.randomUUID().toString()};

		for (int i = 0; i < 1_000_000; i++) {
			ptmt.setString(1, UUID.randomUUID().toString());
			ptmt.setString(2, "{}");
			ptmt.setArray(3, connection.createArrayOf("text", values));

			ptmt.executeUpdate();
		}

		connection.commit();
	}
}
