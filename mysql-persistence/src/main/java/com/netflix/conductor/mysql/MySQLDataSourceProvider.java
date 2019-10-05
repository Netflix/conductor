package com.netflix.conductor.mysql;

import com.netflix.conductor.sql.SQLConfiguration;
import com.netflix.conductor.sql.SQLDataSourceProvider;

import javax.inject.Inject;
import java.nio.file.Paths;

public class MySQLDatasourceProvider extends SQLDataSourceProvider {

  @Inject
  public MySQLDatasourceProvider(SQLConfiguration configuration) {
    super(configuration);
  }

  @Override
  protected String getMigrationPath() {
    return Paths.get("db", "migration", "mysql").toString();
  }

}
