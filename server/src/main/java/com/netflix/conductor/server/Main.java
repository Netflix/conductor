/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.server;

import com.netflix.conductor.aurora.FlywayService;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.lang.management.ManagementFactory;
import java.util.Properties;

/**
 * @author Viren
 * Entry point for the server
 */
public class Main {
	private static final String FLYWAY_MIGRATE = "FLYWAY_MIGRATE";
	private static Logger logger = LoggerFactory.getLogger(Main.class);

	static {
		// Workaround to send java util logging to log4j
		java.util.logging.LogManager.getLogManager().reset();
		org.slf4j.bridge.SLF4JBridgeHandler.removeHandlersForRootLogger();
		org.slf4j.bridge.SLF4JBridgeHandler.install();
		java.util.logging.Logger.getLogger("global").setLevel(java.util.logging.Level.FINEST);
	}

	public static void main(String[] args) throws Exception {
		boolean doMigration = "true".equalsIgnoreCase(System.getenv().get(FLYWAY_MIGRATE));
		if (doMigration) {
			FlywayService.migrate();
		} else {
			logger.info("Skipping Flyway migration (not enabled)");
		}

		if (args.length > 0) {
			String propertyFile = args[0];
			FileInputStream propFile = new FileInputStream(propertyFile);
			Properties props = new Properties(System.getProperties());
			props.load(propFile);
			System.setProperties(props);
		}

		if (args.length == 2) {
			PropertyConfigurator.configure(new FileInputStream(new File(args[1])));
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			int mb = 1024 * 1024;

			public void run() {

				com.sun.management.OperatingSystemMXBean mxBean =
					(com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
				double cpuLoad = mxBean.getProcessCpuLoad();

				Runtime runtime = Runtime.getRuntime();
				StringBuilder info = new StringBuilder();
				info.append("Used memory=").append((runtime.totalMemory() - runtime.freeMemory()) / mb).append("mb");
				info.append(", free memory=").append(runtime.freeMemory() / mb).append("mb");
				info.append(", cpu load=").append(String.format("%.2f", cpuLoad)).append("%");

				logger.info("Caught shutdown signal. " + info.toString());
			}
		});

		ConductorConfig config = new ConductorConfig();
		ConductorServer server = new ConductorServer(config);

		server.start(config.getIntProperty("port", 8080), true);
	}
}