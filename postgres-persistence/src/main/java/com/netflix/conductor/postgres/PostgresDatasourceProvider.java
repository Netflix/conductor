package com.netflix.conductor.postgres;

import com.netflix.conductor.sql.SQLConfiguration;
import com.netflix.conductor.sql.SQLDataSourceProvider;

import javax.inject.Inject;
import java.nio.file.Paths;

public class PostgresDatasourceProvider extends SQLDataSourceProvider {

  @Inject
  public PostgresDatasourceProvider(SQLConfiguration configuration) {
    super(configuration);
  }

  @Override
  protected String getMigrationPath() {
    return Paths.get("db", "migration", "postgres").toString();
  }

}
