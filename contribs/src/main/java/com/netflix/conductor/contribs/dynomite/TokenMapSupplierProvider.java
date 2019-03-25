package com.netflix.conductor.contribs.dynomite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.lb.AbstractTokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TokenMapSupplierProvider implements Provider<TokenMapSupplier> {
    private final HostTokenSupplier hostTokenSupplier;
    private final ObjectMapper objectMapper;

    @Inject
    public TokenMapSupplierProvider(HostTokenSupplier hostTokenSupplier, ObjectMapper objectMapper) {
        this.hostTokenSupplier = hostTokenSupplier;
        this.objectMapper = objectMapper;
    }

    @Override
    public TokenMapSupplier get() {
        return new StaticTopologyTokenMapSupplier(hostTokenSupplier.getHostsTokens(), objectMapper);
    }

    private static class StaticTopologyTokenMapSupplier extends AbstractTokenMapSupplier {

        private final String jsonTopology;

        StaticTopologyTokenMapSupplier(List<HostToken> hostTokens, ObjectMapper objectMapper) {
            this.jsonTopology = convertToJson(buildTokenHosts(hostTokens), objectMapper);
        }

        @Override
        public String getTopologyJsonPayload(Set<Host> activeHosts) {
            return jsonTopology;
        }

        @Override
        public String getTopologyJsonPayload(String hostname) {
            return jsonTopology;
        }

        private List<TokenHost> buildTokenHosts(List<HostToken> hostTokens) {
            return hostTokens.stream()
                    .map(this::convertHostTokenToTokenHost)
                    .collect(Collectors.toList());
        }

        private TokenHost convertHostTokenToTokenHost(HostToken hostToken) {
            Host host = hostToken.getHost();
            return new TokenHost(hostToken.getToken().toString(),
                    host.getHostName(),
                    host.getRack(),
                    host.getDatacenter());
        }

        private String convertToJson(List<TokenHost> tokenHosts, ObjectMapper objectMapper) {
            try {
                return objectMapper.writeValueAsString(tokenHosts);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Error when generating dynomite json topology", e);
            }
        }
    }

    private static class TokenHost {
        private String token;
        private String hostname;
        private String zone;
        private String dc;

        public TokenHost() {
        }

        TokenHost(String token, String hostname, String zone, String dc) {
            this.token = token;
            this.hostname = hostname;
            this.zone = zone;
            this.dc = dc;
        }

        public String getToken() {
            return token;
        }

        public String getHostname() {
            return hostname;
        }

        public String getZone() {
            return zone;
        }

        public String getDc() {
            return dc;
        }
    }
}
