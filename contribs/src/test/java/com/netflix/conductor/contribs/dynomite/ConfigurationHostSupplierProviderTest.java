package com.netflix.conductor.contribs.dynomite;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfigurationHostSupplierProviderTest {

    @Test
    public void shouldReturnHostsFromHostTokenSupplier() {
        HostTokenSupplier hostTokenSupplier = mock(HostTokenSupplier.class);
        List<HostToken> hostTokens = HostTokenHelper.buildHostTokens();
        when(hostTokenSupplier.getHostsTokens()).thenReturn(hostTokens);

        List<Host> expectedHosts = hostTokens.stream().map(HostToken::getHost).collect(Collectors.toList());
        ConfigurationHostSupplierProvider hostSupplier = new ConfigurationHostSupplierProvider(hostTokenSupplier);
        assertEquals(expectedHosts, hostSupplier.get().getHosts());
    }
}
