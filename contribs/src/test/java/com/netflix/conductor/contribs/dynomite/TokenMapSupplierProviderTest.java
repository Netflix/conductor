package com.netflix.conductor.contribs.dynomite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TokenMapSupplierProviderTest {

    @Test
    public void shouldBuildTokenMapSupplier() {
        HostTokenSupplier hostTokenSupplier = mock(HostTokenSupplier.class);
        List<HostToken> hostTokens = HostTokenHelper.buildHostTokens();
        List<Host> hosts = hostTokens.stream().map(HostToken::getHost).collect(Collectors.toList());
        when(hostTokenSupplier.getHostsTokens()).thenReturn(hostTokens);

        ObjectMapper objectMapper = new ObjectMapper();
        TokenMapSupplierProvider tokenMapSupplier = new TokenMapSupplierProvider(hostTokenSupplier, objectMapper);

        Set<Host> activeHosts = hostTokens.stream().map(HostToken::getHost).collect(Collectors.toSet());

        assertTrue(tokenMapSupplier.get().getTokens(activeHosts).containsAll(hostTokens));
        assertEquals(hostTokens.get(0), tokenMapSupplier.get().getTokenForHost(hosts.get(0), activeHosts));
        assertEquals(hostTokens.get(1), tokenMapSupplier.get().getTokenForHost(hosts.get(1), activeHosts));
        assertEquals(hostTokens.get(2), tokenMapSupplier.get().getTokenForHost(hosts.get(2), activeHosts));
    }
}
