package com.netflix.conductor.es6.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;

@EnableConfigurationProperties(ElasticSearchProperties.class)
@Configuration
public class IsTcpProtocol implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        String url = context.getEnvironment().getProperty("conductor.elasticsearch.url");
        if (url.startsWith("http") || url.startsWith("https")) {
            return false;
        }
        return true;
    }
}

