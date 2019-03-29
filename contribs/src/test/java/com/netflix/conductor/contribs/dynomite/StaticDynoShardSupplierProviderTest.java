package com.netflix.conductor.contribs.dynomite;

import com.netflix.conductor.dyno.DynomiteConfiguration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StaticDynoShardSupplierProviderTest {

    @Test
    public void shouldGetShardsFromAvailabilityZone() {
        HostSupplier hostSupplier = mock(HostSupplier.class);
        DynomiteConfiguration configuration = mock(DynomiteConfiguration.class);

        List<Host> hosts = HostTokenHelper.buildHosts();
        String rack = hosts.get(1).getRack();
        String dc = hosts.get(1).getDatacenter();

        when(hostSupplier.getHosts()).thenReturn(hosts);
        when(configuration.getAvailabilityZone()).thenReturn(rack);
        when(configuration.getRegion()).thenReturn(dc);

        StaticDynoShardSupplierProvider shardSupplier = new StaticDynoShardSupplierProvider(hostSupplier, configuration);

        String expectedShard = rack.replaceAll(dc, "");
        assertEquals(newHashSet(expectedShard), shardSupplier.get().getQueueShards());
        assertEquals(expectedShard, shardSupplier.get().getCurrentShard());
    }
}
