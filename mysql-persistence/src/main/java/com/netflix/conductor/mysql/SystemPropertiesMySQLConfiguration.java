package com.netflix.conductor.mysql;

import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class SystemPropertiesMySQLConfiguration extends SystemPropertiesConfiguration implements MySQLConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(SystemPropertiesMySQLConfiguration.class);
    private AWSSecret awsSecret = new AWSSecret();

    public String getJdbcUserName() {
        final String fromAWS = awsSecret.getUsername();
        if (isBlank(fromAWS)) {
            return MySQLConfiguration.super.getJdbcUserName();
        }
        logger.info("username from AWS");
        return fromAWS;
    }

    public String getJdbcPassword() {
        final String fromAWS = awsSecret.getPassword();
        if (isBlank(fromAWS)) {
            return MySQLConfiguration.super.getJdbcPassword();
        }
        logger.info("password from AWS");
        return fromAWS;
    }
}
