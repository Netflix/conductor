package com.netflix.conductor.contribs.dynomite;

import com.netflix.conductor.dyno.DynomiteConfiguration;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfigurationHostTokenSupplierProviderTest {

    private DynomiteConfiguration configuration;

    @Before
    public void setUp() {
        this.configuration = mock(DynomiteConfiguration.class);
    }

    @Test
    public void shouldParseHostsWithTokenFromConfiguration() {
        when(configuration.getHosts()).thenReturn(HostTokenHelper.buildHostTokensConfiguration());

        ConfigurationHostTokenSupplierProvider hostTokenSupplier = new ConfigurationHostTokenSupplierProvider(configuration);
        List<HostToken> expectedHostTokens = HostTokenHelper.buildHostTokens();
        assertEquals(expectedHostTokens, hostTokenSupplier.get().getHostsTokens());
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrownExceptionWhenHostsConfigurationIsNull() {
        when(configuration.getHosts()).thenReturn(null);

        new ConfigurationHostTokenSupplierProvider(configuration).get().getHostsTokens();
    }
}
