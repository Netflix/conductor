package com.netflix.conductor.aurora;

import org.flywaydb.core.Flyway;

import java.io.IOException;
import java.util.Optional;

public class FlywayService {

    public static final String PATH_TO_MIGRATIONS = "classpath:db/migrations";
    public static final String BASELINE_VERSION = "1";

    public static void migrate() throws IOException {
        String dbName = Optional.ofNullable(System.getenv("aurora_db")).orElse( "");
        String hostname = System.getenv("aurora_host");
        String port = System.getenv("aurora_port");
        String username = System.getenv("aurora_user");
        String password = System.getenv("aurora_password");
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
