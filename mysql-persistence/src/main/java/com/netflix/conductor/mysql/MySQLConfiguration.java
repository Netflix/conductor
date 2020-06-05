package com.netflix.conductor.mysql;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import com.netflix.conductor.core.config.Configuration;
import org.apache.commons.lang3.StringUtils;

public interface MySQLConfiguration extends Configuration {

	String JDBC_URL_PROPERTY_NAME = "jdbc.url";
	String JDBC_URL_DEFAULT_VALUE = "jdbc:mysql://localhost:3306/conductor";

	String JDBC_USER_NAME_PROPERTY_NAME = "jdbc.username";
	String JDBC_USER_NAME_DEFAULT_VALUE = "conductor";

	String JDBC_PASSWORD_PROPERTY_NAME = "jdbc.password";
	String JDBC_PASSWORD_DEFAULT_VALUE = "password";

	// If the target database already contains conductor tables but have yet to be
	// versioned in flyway or migrations were manually applied, then the flyway.baseline.on.migrate
	// and flyway.baseline.version properties can be set in order to migrate to the latest tables.
	// The flyway.enabled property should also be set to true. The flyway.baseline.on.migrate property
	// should be set to true and the flyway.baseline.version property should be set to the migration
	// version that the target database is already at. For example, if the V1 schema was manually applied,
	// flyway.baseline.version should be set to 1. When conductor is started with flyway.enabled = true,
	// conductor will apply all migration versions *after* V1.
	String FLYWAY_BASELINE_PROPERTY_NAME = "flyway.baseline.on.migrate";
	boolean FLYWAY_BASELINE_DEFAULT_VALUE = false;

	String FLYWAY_BASELINE_VERSION_PROPERTY_NAME = "flyway.baseline.version";
	int FLYWAY_BASELINE_VERSION_DEFAULT_VALUE = 0;

	String FLYWAY_ENABLED_PROPERTY_NAME = "flyway.enabled";
	boolean FLYWAY_ENABLED_DEFAULT_VALUE = true;

	String FLYWAY_TABLE_PROPERTY_NAME = "flyway.table";

	String FLYWAY_LOCATIONS_PROPERTY_NAME = "flyway.locations";
	String FLYWAY_DEFAULT_LOCATION = "db/migration_conductor";

	// The defaults are currently in line with the HikariConfig defaults, which are unfortunately private.
	String CONNECTION_POOL_MAX_SIZE_PROPERTY_NAME = "conductor.mysql.connection.pool.size.max";
	int CONNECTION_POOL_MAX_SIZE_DEFAULT_VALUE = -1;

	String CONNECTION_POOL_MINIMUM_IDLE_PROPERTY_NAME = "conductor.mysql.connection.pool.idle.min";
	int CONNECTION_POOL_MINIMUM_IDLE_DEFAULT_VALUE = -1;

	String CONNECTION_MAX_LIFETIME_PROPERTY_NAME = "conductor.mysql.connection.lifetime.max";
	long CONNECTION_MAX_LIFETIME_DEFAULT_VALUE = TimeUnit.MINUTES.toMillis(30);

	String CONNECTION_IDLE_TIMEOUT_PROPERTY_NAME = "conductor.mysql.connection.idle.timeout";
	long CONNECTION_IDLE_TIMEOUT_DEFAULT_VALUE = TimeUnit.MINUTES.toMillis(10);

	String CONNECTION_TIMEOUT_PROPERTY_NAME = "conductor.mysql.connection.timeout";
	long CONNECTION_TIMEOUT_DEFAULT_VALUE = TimeUnit.SECONDS.toMillis(30);

	String ISOLATION_LEVEL_PROPERTY_NAME = "conductor.mysql.transaction.isolation.level";
	String ISOLATION_LEVEL_DEFAULT_VALUE = "";

	String AUTO_COMMIT_PROPERTY_NAME = "conductor.mysql.autocommit";
	// This is consistent with the current default when building the Hikari Client.
	boolean AUTO_COMMIT_DEFAULT_VALUE = false;

	default String getJdbcUrl() {
		return getProperty(JDBC_URL_PROPERTY_NAME, JDBC_URL_DEFAULT_VALUE);
	}

	default String getJdbcUserName() {
		return getProperty(JDBC_USER_NAME_PROPERTY_NAME, JDBC_USER_NAME_DEFAULT_VALUE);
	}

	default String getJdbcPassword() {
		return getProperty(JDBC_PASSWORD_PROPERTY_NAME, JDBC_PASSWORD_DEFAULT_VALUE);
	}

	default boolean isFlywayEnabled() {
		return getBoolProperty(FLYWAY_ENABLED_PROPERTY_NAME, FLYWAY_ENABLED_DEFAULT_VALUE);
	}

	default boolean isFlywayBaselineOnMigrate() {
		return getBoolProperty(FLYWAY_BASELINE_PROPERTY_NAME, FLYWAY_BASELINE_DEFAULT_VALUE);
	}

	default Optional<String> getFlywayTable() {
		return Optional.ofNullable(getProperty(FLYWAY_TABLE_PROPERTY_NAME, null));
	}

	default String[] getFlywayLocations() {
		String locationProp = StringUtils.trimToNull(getProperty(FLYWAY_LOCATIONS_PROPERTY_NAME, FLYWAY_DEFAULT_LOCATION));
		if (null == locationProp) {
			return new String[] {};
		}

		if (locationProp.contains(",")) {
			return Arrays.stream(StringUtils.split(locationProp, ","))
						 .map(StringUtils::trimToNull)
						 .filter(Objects::nonNull)
						 .collect(Collectors.toList())
						 .toArray(new String[] {});

		}

		return new String[] {locationProp};
	}

	default int getFlywayBaselineVersion() {
		return getIntProperty(FLYWAY_BASELINE_VERSION_PROPERTY_NAME, FLYWAY_BASELINE_VERSION_DEFAULT_VALUE);
	}

	default int getConnectionPoolMaxSize() {
		return getIntProperty(CONNECTION_POOL_MAX_SIZE_PROPERTY_NAME, CONNECTION_POOL_MAX_SIZE_DEFAULT_VALUE);
	}

	default int getConnectionPoolMinIdle() {
		return getIntProperty(CONNECTION_POOL_MINIMUM_IDLE_PROPERTY_NAME, CONNECTION_POOL_MINIMUM_IDLE_DEFAULT_VALUE);
	}

	default long getConnectionMaxLifetime() {
		return getLongProperty(CONNECTION_MAX_LIFETIME_PROPERTY_NAME, CONNECTION_MAX_LIFETIME_DEFAULT_VALUE);
	}

	default long getConnectionIdleTimeout() {
		return getLongProperty(CONNECTION_IDLE_TIMEOUT_PROPERTY_NAME, CONNECTION_IDLE_TIMEOUT_DEFAULT_VALUE);
	}

	default long getConnectionTimeout() {
		return getLongProperty(CONNECTION_TIMEOUT_PROPERTY_NAME, CONNECTION_TIMEOUT_DEFAULT_VALUE);
	}

	default String getTransactionIsolationLevel() {
		return getProperty(ISOLATION_LEVEL_PROPERTY_NAME, ISOLATION_LEVEL_DEFAULT_VALUE);
	}

	default boolean isAutoCommit() {
		return getBoolProperty(AUTO_COMMIT_PROPERTY_NAME, AUTO_COMMIT_DEFAULT_VALUE);
	}
}
