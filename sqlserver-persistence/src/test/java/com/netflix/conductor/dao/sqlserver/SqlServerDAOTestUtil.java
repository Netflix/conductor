package com.netflix.conductor.dao.sqlserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.sqlserver.config.SqlServerProperties;
import com.netflix.conductor.sqlserver.config.SqlServerProperties.QUEUE_STRATEGY;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;

import javax.sql.DataSource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;


@SuppressWarnings("Duplicates")
public class SqlServerDAOTestUtil {
    private static final Logger logger = LoggerFactory.getLogger(SqlServerDAOTestUtil.class);
    private final HikariDataSource dataSource;
    private final SqlServerProperties properties;
    private final ObjectMapper objectMapper;

    SqlServerDAOTestUtil(MSSQLServerContainer container, ObjectMapper objectMapper, String dbName) throws Exception {
        properties = mock(SqlServerProperties.class);
        String url = container.getJdbcUrl() + ";encrypt=false;trustServerCertificate=true;";
        when(properties.getJdbcUrl()).thenReturn(url);
        when(properties.getJdbcUsername()).thenReturn(container.getUsername());
        when(properties.getJdbcPassword()).thenReturn(container.getPassword());
        when(properties.getTaskDefCacheRefreshInterval()).thenReturn(Duration.ofSeconds(60));
        when(properties.getZone()).thenReturn("GLBL");
        when(properties.getLockNamespace()).thenReturn("GLBL");
        when(properties.getQueueShardingStrategyAsEnum()).thenReturn(QUEUE_STRATEGY.SHARED);
        when(properties.getProcessAllRemovesInterval()).thenReturn(Duration.ofSeconds(0));
        this.objectMapper = objectMapper;
        createDatabase(url, container.getUsername(), container.getPassword(), dbName);
        this.dataSource = getDataSource(properties);
    }

    private void createDatabase(String jdbcUrl, String username, String password, String dbName) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
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

    private HikariDataSource getDataSource(SqlServerProperties properties) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(properties.getJdbcUrl());
        dataSource.setUsername(properties.getJdbcUsername());
        dataSource.setPassword(properties.getJdbcPassword());
        dataSource.setAutoCommit(false);

        // Prevent DB from getting exhausted during rapid testing
        dataSource.setMaximumPoolSize(8);

        flywayMigrate(dataSource);

        return dataSource;
    }

    private void flywayMigrate(DataSource dataSource) {
        
        Flyway flyway = new Flyway();
        flyway.setDataSource(dataSource);
        flyway.setPlaceholderReplacement(false);
        flyway.setLocations(Paths.get("db","migration_sqlserver").toString());
        flyway.setSchemas("data");
        flyway.migrate();
    }

    public HikariDataSource getDataSource() {
        return dataSource;
    }

    public SqlServerProperties getTestProperties() {
        return properties;
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
