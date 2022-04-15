package com.netflix.conductor.aurora;

import com.netflix.conductor.aurora.utils.VaultConfig;
import org.flywaydb.core.Flyway;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

public class FlywayService {

    public static final String PATH_TO_MIGRATIONS = "classpath:db/migrations";
    public static final String BASELINE_VERSION = "1";

    public static void migrate() throws IOException {
        Properties props = VaultConfig.getInstance().getProperties();
        String dbName = Optional.ofNullable(props.getProperty("aurora_db")).orElse( "");
        String hostname = props.getProperty("aurora_host");
        String port = props.getProperty("aurora_port");
        String username = props.getProperty("aurora_user");
        String password = props.getProperty("aurora_password");
        String url = "jdbc:postgresql://" + hostname + ":" + port + "/" + dbName;

        Flyway flyway = Flyway.configure()
                .dataSource(url, username, password)
                .locations(PATH_TO_MIGRATIONS)
                .baselineOnMigrate(true)
                .baselineVersion(BASELINE_VERSION)
                .load();
        flyway.migrate();
    }
}
