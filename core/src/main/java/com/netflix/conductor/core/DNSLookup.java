package com.netflix.conductor.core;
import com.netflix.conductor.service.MetricService;
import org.apache.commons.lang3.ArrayUtils;
import org.xbill.DNS.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * Created by hhwang on 6/15/2017.
 */
public class DNSLookup {
    public DNSLookup(){}

    public static String lookup(String service) {
        long sd_start_time = System.currentTimeMillis();
        long sd_lookup_time = -1;
        try {
            DNSLookup lookup = new DNSLookup();
            DNSLookup.DNSResponses responses = lookup.lookupService(service);
            if (responses != null && ArrayUtils.isNotEmpty(responses.getResponses())) {
                String address = responses.getResponses()[0].getAddress();
                int port = responses.getResponses()[0].getPort();
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

    public DNSResponses lookupService(String query) {
        DNSResponses responses = new DNSResponses();
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
                    DNSResponse r = new DNSResponse(hostname, port);
                    r.setAddress(address.getHostAddress());
                    responses.addResponse(r);
                }
            }
        } catch (TextParseException | UnknownHostException e) {
            e.printStackTrace();
        }
        return responses;
    }

    public class DNSResponse {

        String hostname;
        String address;
        int port;

        public DNSResponse(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
        }

        public void setHostName(String hostname) {
            this.hostname = hostname;
        }
        public String getHostName() {
            return this.hostname;
        }

        public void setAddress(String address) {
            this.address = address;
        }
        public String getAddress() {
            return this.address;
        }


        public void setPort(int port) {
            this.port = port;
        }
        public int getPort() {
            return this.port;
        }
    }

    public class DNSResponses {

        DNSResponse[] responses;

        public DNSResponses() {};

        public void setResponses(DNSResponse[] r) {
            this.responses = r;
        }
        public DNSResponse[] getResponses() {
            return this.responses;
        }

        public void addResponse(DNSResponse response) {
            if (responses == null) {
                responses = new DNSResponse[1];
            }
            else {
                responses = Arrays.copyOf(responses, responses.length+1);
            }
            responses[responses.length-1] = response;
        }
    }
}