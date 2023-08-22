/*
 * Copyright 2021 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor;

import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.io.FileSystemResource;

// Prevents from the datasource beans to be loaded, AS they are needed only for specific databases.
// In case that SQL database is selected this class will be imported back in the appropriate
// database persistence module.
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
@ComponentScan(basePackages = {"com.netflix.conductor", "io.orkes.conductor"})
public class Conductor {

    private static final Logger log = LoggerFactory.getLogger(Conductor.class);

    public static void main(String[] args) throws IOException {
        loadExternalConfig();

        SpringApplication.run(Conductor.class, args);
    }

    /**
     * Reads properties from the location specified in <code>CONDUCTOR_CONFIG_FILE</code> and sets
     * them as system properties so they override the default properties.
     *
     * <p>Spring Boot property hierarchy is documented here,
     * https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-external-config
     *
     * @throws IOException if file can't be read.
     */
    private static void loadExternalConfig() throws IOException {
        String configFile = System.getProperty("CONDUCTOR_CONFIG_FILE");
        if (StringUtils.isNotBlank(configFile)) {
            FileSystemResource resource = new FileSystemResource(configFile);
            if (resource.exists()) {
                Properties properties = new Properties();
                properties.load(resource.getInputStream());
                properties.forEach(
                        (key, value) -> {
                            String resolved = resolveValueWithEnvVars((String) value);
                            System.setProperty((String) key, resolved);
                        });
                log.info("Loaded {} properties from {}", properties.size(), configFile);
            } else {
                log.warn("Ignoring {} since it does not exist", configFile);
            }
        }
    }

    /**
     * Resolve environment variables in a property value.
     *
     * <p>idea taken from here
     * https://www.rgagnon.com/javadetails/java-resolve-environment-variable-in-property-value.html
     *
     * <p>this will replace "${TEMP}" with the environment variables (if exists)
     */
    private static String resolveValueWithEnvVars(String value) {
        if (null == value) {
            return null;
        }

        Pattern p = Pattern.compile("\\$\\{(\\w+)\\}|\\$(\\w+)");
        Matcher m = p.matcher(value);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String envVarName = null == m.group(1) ? m.group(2) : m.group(1);
            String envVarValue = System.getenv(envVarName);
            m.appendReplacement(
                    sb, null == envVarValue ? "" : Matcher.quoteReplacement(envVarValue));
        }
        m.appendTail(sb);
        return sb.toString();
    }
}
