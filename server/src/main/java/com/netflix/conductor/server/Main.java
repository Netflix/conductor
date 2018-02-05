/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	static {
		// Workaround to send java util logging to log4j
		java.util.logging.LogManager.getLogManager().reset();
		org.slf4j.bridge.SLF4JBridgeHandler.removeHandlersForRootLogger();
		org.slf4j.bridge.SLF4JBridgeHandler.install();
		java.util.logging.Logger.getLogger("global").setLevel(java.util.logging.Level.FINEST);
	}

	public static void main(String[] args) throws Exception {

		if(args.length > 0) {
			String propertyFile = args[0];	
			System.out.println("Using " + propertyFile);
			FileInputStream propFile = new FileInputStream(propertyFile);
			Properties props = new Properties(System.getProperties());
			props.load(propFile);
			System.setProperties(props);
		}
		
		
		
		if(args.length == 2) {
			System.out.println("Using log4j config " + args[1]);
			PropertyConfigurator.configure(new FileInputStream(new File(args[1])));
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			int mb = 1024*1024;
			public void run() {

				Runtime runtime = Runtime.getRuntime();
				StringBuilder info = new StringBuilder();
				info.append("Used memory=").append((runtime.totalMemory() - runtime.freeMemory()) / mb);
				info.append(", free memory=").append(runtime.freeMemory() / mb).append("mb");

				logger.info("Caught shutdown signal. " + info.toString());
			}
		});

		ConductorConfig config = new ConductorConfig();
		ConductorServer server = new ConductorServer(config);
		
		System.out.println("\n\n\n");
		System.out.println("                     _            _             ");
		System.out.println("  ___ ___  _ __   __| |_   _  ___| |_ ___  _ __ ");
		System.out.println(" / __/ _ \\| '_ \\ / _` | | | |/ __| __/ _ \\| '__|");
		System.out.println("| (_| (_) | | | | (_| | |_| | (__| || (_) | |   ");
		System.out.println(" \\___\\___/|_| |_|\\__,_|\\__,_|\\___|\\__\\___/|_|   ");
		System.out.println("\n\n\n");

		server.start(config.getIntProperty("port", 8080), true);
	}
}
