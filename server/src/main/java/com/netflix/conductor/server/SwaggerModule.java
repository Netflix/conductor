package com.netflix.conductor.server;

import com.google.inject.Scopes;
import com.google.inject.servlet.ServletModule;

import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import java.util.HashMap;
import java.util.Map;

public class SwaggerModule extends ServletModule {

    @Override
    protected void configureServlets() {
        bind(GuiceContainer.class).in(Scopes.SINGLETON);
        Map<String, String> params = new HashMap<>();
        params.put("resourceBase", getResourceBasePath());
        params.put("redirectWelcome", "true");
        serve("/*").with(GuiceContainer.class, params);
    }

    private String getResourceBasePath() {
        return SwaggerModule.class.getResource("/swagger-ui").toExternalForm();
    }
}
