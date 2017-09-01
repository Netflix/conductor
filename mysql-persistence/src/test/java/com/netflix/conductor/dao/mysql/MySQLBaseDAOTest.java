package com.netflix.conductor.dao.mysql;

import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.netflix.conductor.config.TestConfiguration;

class MySQLBaseDAOTest {

	private final List<String> TABLE_NAMES = loadTableNames();
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	protected final Sql2o testSql2o;
	protected final TestConfiguration testConfiguration = new TestConfiguration();
	protected final ObjectMapper objectMapper = createObjectMapper();

	MySQLBaseDAOTest() {
		Boolean useMySQLForUnitTests = Boolean.valueOf(System.getProperty("useMySQLForUnitTests", "false"));
		if (useMySQLForUnitTests) {
			logger.info("********************************************************************************");
			logger.info("Using MySQL for running the unit tests using default parameters:");
			logger.info(" - url: jdbc:mysql://localhost:3306/conductor");
			logger.info(" - username: conductor");
			logger.info(" - password: password");
			logger.info("********************************************************************************");
		} else {
			testConfiguration.setProperty("jdbc.url", "jdbc:h2:mem:test;MODE=MySQL");
			testConfiguration.setProperty("jdbc.username", "");
			testConfiguration.setProperty("jdbc.password", "");
		}
		testSql2o = new MySQLESWorkflowModule().getSql2o(testConfiguration);
	}

	private ObjectMapper createObjectMapper() {
		ObjectMapper om = new ObjectMapper();
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
		om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		return om;
	}

	protected void resetAllData() {
		logger.info("Resetting data for test");
		try(Connection connection = testSql2o.open()) {
			TABLE_NAMES.forEach(table -> connection.createQuery("TRUNCATE TABLE " + table).executeUpdate());
		}
	}

	private static List<String> loadTableNames() {
		Pattern p = Pattern.compile("CREATE TABLE (\\w+)", Pattern.MULTILINE|Pattern.DOTALL);
		String schemaFile = new Scanner(MySQLBaseDAOTest.class.getResourceAsStream("/db/migration/V1__initial_schema.sql")).useDelimiter("\\Z").next();
		Matcher m = p.matcher(schemaFile);
		List<String> tableNames = Lists.newArrayList();
		while( m.find() ) {
			tableNames.add(m.group(1));
		}
		return tableNames;
	}
}
