/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.dao.sqlserver;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.config.TestConfiguration;
import com.netflix.conductor.core.config.Configuration;
import com.zaxxer.hikari.HikariDataSource;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("Duplicates")
public class SqlServerDAOTestUtil {
    private static final Logger logger = LoggerFactory.getLogger(SqlServerDAOTestUtil.class);
    private final HikariDataSource dataSource;
    private final TestConfiguration testConfiguration = new TestConfiguration();
    private final ObjectMapper objectMapper = new JsonMapperProvider().get();

    SqlServerDAOTestUtil(String dbName) throws Exception {
        testConfiguration.setProperty("jdbc.url", "jdbc:sqlserver://localhost:1433;database="+dbName+";encrypt=false;trustServerCertificate=true;");
        testConfiguration.setProperty("jdbc.username", "sa");
        testConfiguration.setProperty("jdbc.password", "Password1");
        createDatabase(dbName);
        this.dataSource = getDataSource(testConfiguration);
    }

    private void createDatabase(String dbName) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:sqlserver://localhost:1433;database=master;encrypt=false;trustServerCertificate=true;");
        dataSource.setUsername("sa");
        dataSource.setPassword("Password1");
        dataSource.setAutoCommit(false);

        dataSource.setMaximumPoolSize(2);
        String CREATE_DATABASE = String.join("\n", 
            "IF NOT EXISTS",
            "(",
            "   SELECT name FROM master.dbo.sysdatabases",
            "   WHERE name = N'%1$s'",
            ")",
            "BEGIN",
            "   CREATE DATABASE [%1$s];",
            "END"
        );
        try (Connection connection = dataSource.getConnection()) {
            try(Statement statement = connection.createStatement()) {
                statement.execute(String.format(CREATE_DATABASE, dbName));
            }
        } catch (SQLException sqlException) {
            logger.error("Unable to create default connection for docker sqlserver db", sqlException);
            throw new RuntimeException(sqlException);
        }finally {
            dataSource.close();
        }
    }

    private HikariDataSource getDataSource(Configuration config) {

        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(config.getProperty("jdbc.url", "jdbc:sqlserver://localhost:1433;database=Conductor;encrypt=false;trustServerCertificate=true;"));
        dataSource.setUsername(config.getProperty("jdbc.username", "conductor"));
        dataSource.setPassword(config.getProperty("jdbc.password", "password"));
        dataSource.setAutoCommit(false);

        // Prevent DB from getting exhausted during rapid testing
        dataSource.setMaximumPoolSize(8);

        flywayMigrate(dataSource);

        return dataSource;
    }

    private void flywayMigrate(DataSource dataSource) {
        Flyway flyway = Flyway.configure()
            .dataSource(dataSource)
            .table("schema_version")
            .placeholderReplacement(false)
            .locations(Paths.get("db","migration_sqlserver").toString())
            .schemas("data")
            .load();

        flyway.migrate();

    }

    public HikariDataSource getDataSource() {
        return dataSource;
    }

    public TestConfiguration getTestConfiguration() {
        return testConfiguration;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public void resetAllData() {
        String TRUNCATE_ALL_TABLES = String.join("\r\n", 
            " declare @ObjName nvarchar(200)",
            " declare @StrSQL nvarchar(1000)",
            " ",
            " declare CurTables cursor read_only",
            " for",
            " select '[' + s.name + '].[' + t.name + ']'",
            "    from sys.tables t",
            "    inner join sys.schemas s",
            "    on t.schema_id = s.schema_id",
            "    where t.type = 'U'",
            " ",
            " open CurTables",
            " fetch next from CurTables into @ObjName",
            " while @@FETCH_STATUS = 0",
            " begin",
            " ",
            "    SET @StrSQL = N'TRUNCATE TABLE ' + @ObjName",
            "        print @StrSQL",
            "        EXEC(@StrSQL)",
            " ",
            "    FETCH NEXT FROM CurTables into @ObjName",
            " END",
            " close CurTables",
            " deallocate CurTables",
            " "
        );
        logger.info("Resetting data for test");
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement truncate_all = connection.prepareStatement(TRUNCATE_ALL_TABLES);
            truncate_all.execute();
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }
}
