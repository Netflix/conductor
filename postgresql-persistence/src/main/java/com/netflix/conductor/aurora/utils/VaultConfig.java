package com.netflix.conductor.aurora.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class VaultConfig {
    private static final String SECRETS = "SECRETS";
    private static VaultConfig INSTANCE;
    private Properties properties;

    private VaultConfig() throws IOException {
        properties = new Properties();
        properties.load(new FileInputStream(get(System.getenv(), SECRETS, "secrets.env")));
    }

    public static VaultConfig getInstance() throws IOException {
        if (INSTANCE == null)
            INSTANCE = new VaultConfig();
        return INSTANCE;
    }

    public Properties getProperties() {
        return properties;
    }

    private static <T> T get(Map<String, T> map, String key, T defaultValue) {
        if (map == null) {
            return defaultValue;
        }
        T result = map.get(key);
        return result == null ? defaultValue : result;
    }
}
