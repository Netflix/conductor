package com.netflix.conductor.postgres;

import com.netflix.conductor.sql.SQLConfiguration;
import com.netflix.conductor.sql.SQLDataSourceProvider;

import javax.inject.Inject;
import java.nio.file.Paths;

public class PostgresDataSourceProvider extends SQLDataSourceProvider {

  @Inject
  public PostgresDataSourceProvider(SQLConfiguration configuration) {
    super(configuration);
  }

  @Override
  protected String getMigrationPath() {
    return Paths.get("db", "migration", "postgres").toString();
  }

}
