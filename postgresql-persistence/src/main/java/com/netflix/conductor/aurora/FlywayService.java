package com.netflix.conductor.aurora;

import com.netflix.conductor.core.config.Configuration;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FlywayService {

    public static final String PATH_TO_MIGRATIONS = "classpath:db/migrations";
    private static final Logger logger = LoggerFactory.getLogger(FlywayService.class);

    public static void migrate(Configuration config) throws Exception{
        String db = config.getProperty("aurora.db", null);
        String host = config.getProperty("aurora.host", null);
        String port = config.getProperty("aurora.port", "5432");
        String user = config.getProperty("aurora.user", null);
        String pwd = config.getProperty("aurora.password", null);
        String baselineVersion = config.getProperty("FLYWAY_BASELINE_VERSION", "1");

        String url = String.format("jdbc:postgresql://%s:%s/%s", host, port, db);

        Flyway flyway = Flyway.configure()
                .dataSource(url, user, pwd)
                .locations(PATH_TO_MIGRATIONS)
                .baselineOnMigrate(true)
                .baselineVersion(baselineVersion)
                .load();
        MigrateResult result = flyway.migrate();

        if ( result.success){
            logger.info("Flyway migration completed successfully. Initial Version:" + result.initialSchemaVersion + " targetVersion:" + result.targetSchemaVersion );
        }else{
            String errmessage = "Flyway migration FAILED. Initial Version:" + result.initialSchemaVersion + " targetVersion:" + result.targetSchemaVersion;
            logger.error(errmessage);
            if (result.warnings != null && result.warnings.size() > 0){
                result.warnings.stream().forEach(w-> logger.error("Warning: " + w));
            }
            throw new Exception(errmessage);
        }

    }
}
