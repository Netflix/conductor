package com.netflix.conductor.dao.sqlserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.config.TestConfiguration;
import com.netflix.conductor.core.config.Configuration;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


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
            "   CREATE DATABASE [%1$s]",
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
        
        Flyway flyway = new Flyway();
        flyway.setDataSource(dataSource);
        flyway.setPlaceholderReplacement(false);
        flyway.setLocations(Paths.get("db","migration_sqlserver").toString());
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

    public void configureInMemoryTableForLock() throws SQLException{
        final String CREATE_AS_MEMORY = String.join("\n", 
            "DROP TABLE [dbo].[reentrant_lock]",
            "CREATE TABLE [dbo].[reentrant_lock] (",
            "lock_id VARCHAR(255) PRIMARY KEY NONCLUSTERED,",
            "holder_id VARCHAR(255) NOT NULL,",
            "expire_time DATETIME2 DEFAULT SYSDATETIME() NOT NULL",
            ")",
            "with ( memory_optimized=on,",
            "durability=schema_and_data",
            ")"
        );

        Connection conn = dataSource.getConnection();
        conn.prepareStatement(CREATE_AS_MEMORY).execute();
    }

    public void configureInMemoryTableForQueue() throws SQLException{
        final String CREATE_AS_MEMORY = String.join("\n", 
            "DROP TABLE [dbo].[queue_message]",
            "CREATE TABLE [dbo].[queue_message] (",
            "   id INT PRIMARY KEY NONCLUSTERED IDENTITY(1, 1),",
            "   created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,",
            "   deliver_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,",
            "   queue_name VARCHAR(255) NOT NULL,",
            "   message_id VARCHAR(255) NOT NULL,",
            "   priority TINYINT DEFAULT 0,",
            "   popped BIT DEFAULT 0,",
            "   offset_time_seconds VARCHAR(64),",
            "   payload NVARCHAR(4000),",
            "   UNIQUE (queue_name, message_id),",
            "   UNIQUE (queue_name, popped, deliver_on, created_on, priority)",
            ")",
            "with ( memory_optimized=on,",
            "durability=schema_and_data",
            ")"
        );

        Connection conn = dataSource.getConnection();
        conn.prepareStatement(CREATE_AS_MEMORY).execute();
    }
}
