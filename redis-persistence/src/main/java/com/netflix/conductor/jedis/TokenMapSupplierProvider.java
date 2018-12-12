package com.netflix.conductor.jedis;

import com.google.common.collect.Lists;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TokenMapSupplierProvider implements Provider<TokenMapSupplier> {
    private final HostSupplier hostSupplier;

    @Inject
    public TokenMapSupplierProvider(HostSupplier hostSupplier) {
        this.hostSupplier = hostSupplier;
    }

    @Override
    public TokenMapSupplier get() {
        return new TokenMapSupplier() {

            List<HostToken> tokens = Lists.newArrayList(hostSupplier.getHosts()).stream().map(it -> new HostToken(1L, it)).collect(Collectors.toList());

            @Override
            public List<HostToken> getTokens(Set<Host> activeHosts) {
                return tokens;
            }

            @Override
            public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
                // FIXME This isn't particularly safe, but it is equivalent to the existing code.
                // FIXME It seems like we should be supply tokens for more than one host?
                return tokens.get(0);
            }
        };
    }

}
