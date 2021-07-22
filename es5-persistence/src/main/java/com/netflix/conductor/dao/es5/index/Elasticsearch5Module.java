/**
 * Copyright 2016 Netflix, Inc.
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
package com.netflix.conductor.dao.es5.index;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.conductor.core.DNSLookup;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.utils.WaitUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * @author Viren
 * Provider for the elasticsearch transport client
 */
public class Elasticsearch5Module extends AbstractModule {

	private static Logger log = LoggerFactory.getLogger(Elasticsearch5Module.class);

	@Provides
	@Singleton
	public Client getClient(Configuration config) throws Exception {
		// Initial sleep to let elasticsearch servers start first
		int initialSleep = config.getIntProperty("workflow.elasticsearch.initial.sleep.seconds", 0);
		if (initialSleep > 0) {
			Uninterruptibles.sleepUninterruptibly(initialSleep, TimeUnit.SECONDS);
		}

		Settings settings = Settings.builder()
				.put("client.transport.ignore_cluster_name", true)
				.put("client.transport.sniff", true).build();
		TransportClient tc = new PreBuiltTransportClient(settings);

		String dnsService = config.getProperty("workflow.elasticsearch.service", null);
		if (StringUtils.isNotEmpty(dnsService)) {
			log.info("Using dns service {} to setup elasticsearch cluster", dnsService);
			int connectAttempts = config.getIntProperty("workflow.elasticsearch.dnslookup.attempts", 60);
			int connectSleepSecs = config.getIntProperty("workflow.elasticsearch.dnslookup.sleep.seconds", 1);

			WaitUtils.wait("dnsLookup(elasticsearch)", connectAttempts, connectSleepSecs, () -> {
				List<TransportAddress> nodes = lookupNodes(dnsService);
				nodes.forEach(node -> {
					log.info("Adding {} to the elasticsearch cluster configuration", node);
					tc.addTransportAddress(node);
				});
				return true;
			});

			log.info("Dns lookup done");
		} else {
			String clusterAddress = config.getProperty("workflow.elasticsearch.url", "");
			if (clusterAddress.equals("")) {
				log.warn("workflow.elasticsearch.url is not set.  Indexing will remain DISABLED.");
			}

			String[] hosts = clusterAddress.split(",");
			for (String host : hosts) {
				String[] hostparts = host.split(":");
				String hostname = hostparts[0];
				int hostport = 9300; // tcp port
				if (hostparts.length == 2) hostport = Integer.parseInt(hostparts[1]);
				tc.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), hostport));
			}
		}

		// Elasticsearch waiter will be in place only when there is at least one server
		if (!tc.transportAddresses().isEmpty()) {
			int connectAttempts = config.getIntProperty("workflow.elasticsearch.connection.attempts", 60);
			int connectSleepSecs = config.getIntProperty("workflow.elasticsearch.connection.sleep.seconds", 1);

			WaitUtils.wait("elasticsearch", connectAttempts, connectSleepSecs, () -> {
				ClusterHealthResponse healthResponse = null;
				try {
					// Get cluster health status
					healthResponse = tc.admin().cluster().prepareHealth().execute().get();
					log.info("Cluster health " + healthResponse.toString());
					return true;
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		}

		// Start the dns service monitor only when dns service specified
		if (StringUtils.isNotEmpty(dnsService)) {
			int clusterSize = config.getIntProperty("workflow.elasticsearch.cluster.size", 3);
			int monitorDelay = config.getIntProperty("workflow.elasticsearch.monitor.delay", 30);
			int monitorPeriod = config.getIntProperty("workflow.elasticsearch.monitor.period.seconds", 3);
			try {
				Executors.newScheduledThreadPool(1)
						.scheduleAtFixedRate(() -> monitor(tc, dnsService, clusterSize), monitorDelay, monitorPeriod, TimeUnit.SECONDS);
			} catch (Exception e) {
				log.error("Unable to start elasticsearch service monitor: {}", e.getMessage(), e);
			}
		}

		return tc;
	}

	private List<TransportAddress> lookupNodes(String dnsService) {
		// NO longer in use
		return Collections.emptyList();
	}

	private void monitor(TransportClient transport, String dnsService, Integer clusterSize) {
		try {
			List<TransportAddress> current = transport.transportAddresses();
			List<TransportAddress> resolved = lookupNodes(dnsService);
			log.debug("Current nodes=" + current + ", resolved nodes=" + resolved);

			// Add new to the configuration
			resolved.forEach(node -> {
				if (!transport.transportAddresses().contains(node)) {
					log.info("Adding node {} to the elasticsearch cluster", node);
					transport.addTransportAddress(node);
				}
			});

			ClusterHealthResponse response = transport.admin().cluster().prepareHealth().execute().get();
			log.debug("Cluster health " + response.getStatus());
			if (response.getStatus() != ClusterHealthStatus.GREEN) {
				log.error("Monitor. Elasticsearch cluster status is " + response.getStatus());
			}

			// Remove old nodes only when cluster reached the green status and
			// the number of current instances is equal to the desired cluster size.
			// Using clusterSize prevents excluding alive nodes due to dns lookup issue (missing nodes in response)
			boolean isHealthOkay = ClusterHealthStatus.GREEN.equals(response.getStatus());
			boolean isClusterSizeOkay = resolved.size() == clusterSize;
			if (isHealthOkay && isClusterSizeOkay) {
				current.forEach(node -> {
					if (!resolved.contains(node)) {
						log.info("Excluding node {} from the elasticsearch cluster", node);
						transport.removeTransportAddress(node);
					}
				});
			}

		} catch (Exception ex) {
			log.error("Monitor failed with " + ex.getMessage(), ex);
		}
	}

	@Override
	protected void configure() {
	}
}
