package com.netflix.conductor.contribs.dynomite;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;

import java.util.Arrays;
import java.util.List;

class HostTokenHelper {
    private static final String HOST_1 = "host1";
    private static final int PORT_1 = 8091;
    private static final String RACK_1 = "rack1";
    private static final String DATACENTER_1 = "datacenter1";
    private static final long TOKEN_1 = 1383429731L;
    private static final String HOST_2 = "host2";
    private static final int PORT_2 = 8092;
    private static final String RACK_2 = "rack2";
    private static final String DATACENTER_2 = "datacenter2";
    private static final String HOST_3 = "host3";
    private static final int PORT_3 = 8093;
    private static final String RACK_3 = "rack3";
    private static final String DATACENTER_3 = "datacenter3";
    private static final long TOKEN_2 = 1383429732L;
    private static final long TOKEN_3 = 1383429733L;

    static List<HostToken> buildHostTokens() {
        List<Host> hosts = buildHosts();
        return Arrays.asList(
                new HostToken(TOKEN_1, hosts.get(0)),
                new HostToken(TOKEN_2, hosts.get(1)),
                new HostToken(TOKEN_3, hosts.get(2)));
    }

    static List<Host> buildHosts() {
        return Arrays.asList(
                new Host(HOST_1, null, PORT_1, RACK_1, DATACENTER_1, Host.Status.Up),
                new Host(HOST_2, null, PORT_2, RACK_2, DATACENTER_2, Host.Status.Up),
                new Host(HOST_3, null, PORT_3, RACK_3, DATACENTER_3, Host.Status.Up));
    }

    static String buildHostTokensConfiguration() {
        return String.format("%s:%d:%s:%s:%d;%s:%d:%s:%s:%d;%s:%d:%s:%s:%d;",
                HOST_1, PORT_1, RACK_1, DATACENTER_1, TOKEN_1,
                HOST_2, PORT_2, RACK_2, DATACENTER_2, TOKEN_2,
                HOST_3, PORT_3, RACK_3, DATACENTER_3, TOKEN_3);
    }
}
