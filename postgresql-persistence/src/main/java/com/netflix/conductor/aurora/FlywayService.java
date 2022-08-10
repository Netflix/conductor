package com.netflix.conductor.aurora;

import com.netflix.conductor.core.config.Configuration;
import org.flywaydb.core.Flyway;

import java.io.IOException;

public class FlywayService {

    public static final String PATH_TO_MIGRATIONS = "classpath:db/migrations";
    public static final String BASELINE_VERSION = "1";

    public static void migrate(Configuration config) throws IOException {
        String db = config.getProperty("aurora.db", null);
        String host = config.getProperty("aurora.host", null);
        String port = config.getProperty("aurora.port", "5432");
        String user = config.getProperty("aurora.user", null);
        String pwd = config.getProperty("aurora.password", null);

        String url = String.format("jdbc:postgresql://%s:%s/%s", host, port, db);

        Flyway flyway = Flyway.configure()
                .dataSource(url, user, pwd)
                .locations(PATH_TO_MIGRATIONS)
                .baselineOnMigrate(true)
                .baselineVersion(BASELINE_VERSION)
                .load();
        flyway.migrate();
    }
}
