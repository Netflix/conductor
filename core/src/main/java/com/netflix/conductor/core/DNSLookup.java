package com.netflix.conductor.core;

import com.netflix.conductor.service.MetricService;
import org.xbill.DNS.*;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by hhwang on 6/15/2017.
 */
public class DNSLookup {
	public DNSLookup() {
	}

	public static String lookup(String service) {
		long sd_start_time = System.currentTimeMillis();
		long sd_lookup_time = -1;
		try {
			DNSLookup lookup = new DNSLookup();
			DNSResponse dnsResponse = lookup.lookupService(service);
			if (dnsResponse != null) {
				String address = dnsResponse.getAddress();
				int port = dnsResponse.getPort();
				sd_lookup_time = System.currentTimeMillis() - sd_start_time;
				return "http://" + address + ":" + port;
			}
			return null;
		} finally {
			// Service Discovery Metric
			MetricService
					.getInstance()
					.serviceDiscovery(service, sd_lookup_time);
		}
	}

	public DNSResponse lookupService(String query) {
		try {
			Lookup lookup = new Lookup(query, Type.SRV);
			Cache cache = Lookup.getDefaultCache(DClass.IN);
			cache.clearCache();
			lookup.setCache(null);
			Record[] records = lookup.run();
			if (records != null) {
				for (Record record : records) {
					SRVRecord srv = (SRVRecord) record;

					String hostname = srv.getTarget().toString().replaceFirst("\\.$", "");
					InetAddress address = Address.getByName(hostname);
					int port = srv.getPort();
					return new DNSResponse(hostname, port, address.getHostAddress());
				}
			}
		} catch (TextParseException | UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static class DNSResponse {
		private final String hostname;
		private final String address;
		private final int port;

		public DNSResponse(String hostname, int port, String address) {
			this.hostname = hostname;
			this.address = address;
			this.port = port;
		}

		public String getHostName() {
			return this.hostname;
		}

		public String getAddress() {
			return this.address;
		}

		public int getPort() {
			return this.port;
		}
	}
}
