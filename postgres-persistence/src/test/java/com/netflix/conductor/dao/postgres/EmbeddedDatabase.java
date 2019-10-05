package com.netflix.conductor.dao.postgres;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import static com.netflix.conductor.dao.postgres.PostgresDAOTestUtil.createDb;

public enum EmbeddedDatabase {
  INSTANCE;

	private final DataSource ds;
  private final Logger logger = LoggerFactory.getLogger(EmbeddedDatabase.class);

  public DataSource getDataSource() {
    return ds;
  }

  private DataSource startEmbeddedDatabase() {
    try {
			DataSource ds = EmbeddedPostgres.builder().setPort(54320).start().getPostgresDatabase();
			createDb(ds,"conductor");
			return ds;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  EmbeddedDatabase() {
		logger.info("Starting embedded database");
		ds = startEmbeddedDatabase();
  }

}
