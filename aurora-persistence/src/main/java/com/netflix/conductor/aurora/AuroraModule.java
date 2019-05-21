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
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * @author Oleksiy Lysak
 *
 */
public class AuroraModule extends AbstractModule {
	private static Logger logger = LoggerFactory.getLogger(AuroraModule.class);

	@Override
	protected void configure() {
        bind(DataSource.class).toProvider(AuroraDataSourceProvider.class).in(Scopes.SINGLETON);
		bind(MetadataDAO.class).to(AuroraMetadataDAO.class);
		bind(ExecutionDAO.class).to(AuroraExecutionDAO.class);
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
		module.test1(dataSource);
	}

	private void test1(HikariDataSource dataSource) throws SQLException {
		String INSERT_TASK = "INSERT INTO task (task_id, json_data) VALUES (?, ?) " +
			" ON CONFLICT ON CONSTRAINT task_task_id DO NOTHING";

		String json = "{\"foo\":\"bar2\"}";
		Connection connection = dataSource.getConnection();
		PreparedStatement statement = connection.prepareStatement(INSERT_TASK);
		statement.setString(1, "1");
		statement.setString(2, json);
		int i = statement.executeUpdate();
		System.out.println("Inserted " + (i > 0));
		connection.commit();
	}
}
