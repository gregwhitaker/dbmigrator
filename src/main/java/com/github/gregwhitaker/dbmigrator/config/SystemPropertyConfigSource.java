package com.github.gregwhitaker.dbmigrator.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Configuration source that retrieves values from Java system properties.
 */
public class SystemPropertyConfigSource implements ConfigSource {
    private static final Logger LOG = LoggerFactory.getLogger(SystemPropertyConfigSource.class);

    /**
     * Enumeration of configuration environment variables.
     */
    public enum SystemProps {
        DB_JDBC_URL("db.jdbcUrl"),
        DB_USERNAME("db.username"),
        DB_PASSWORD("db.password"),
        DB_ENV("db.env"),
        DB_ENV_SPRINGBOOT("spring.profiles.active"),
        DB_CLEAN_MIGRATE("db.cleanMigrate"),
        DB_CLEAN_NO_MIGRATE("db.cleanNoMigrate");

        private final String value;

        SystemProps(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    public void resolve(DatabaseMigratorConfig config) {
        LOG.debug("Resolving configuration properties via system properties");

        if (System.getProperty(SystemProps.DB_JDBC_URL.getValue()) != null) {
            config.setJdbcUrl(System.getProperty(SystemProps.DB_JDBC_URL.getValue()));
        }

        if (System.getProperty(SystemProps.DB_USERNAME.getValue()) != null) {
            config.setUsername(System.getProperty(SystemProps.DB_USERNAME.getValue()));
        }

        if (System.getProperty(SystemProps.DB_PASSWORD.getValue()) != null) {
            config.setPassword(System.getProperty(SystemProps.DB_PASSWORD.getValue()));
        }

        if (System.getProperty(SystemProps.DB_ENV.getValue()) != null) {
            config.setEnvironment(System.getProperty(SystemProps.DB_ENV.getValue()));
        } else {
            // Support spring boot active profiles for setting the environment as well as db.env
            if (System.getProperty(SystemProps.DB_ENV_SPRINGBOOT.getValue()) != null) {
                config.setEnvironment(resolveEnvironmentFromSpringBootActiveProfiles(System.getProperty(SystemProps.DB_ENV_SPRINGBOOT.getValue())));
            }
        }

        if (System.getProperty(SystemProps.DB_CLEAN_MIGRATE.getValue()) != null) {
            config.setCleanMigrate(Boolean.parseBoolean(System.getProperty(SystemProps.DB_CLEAN_MIGRATE.getValue())));
        }

        if (System.getProperty(SystemProps.DB_CLEAN_NO_MIGRATE.getValue()) != null) {
            config.setCleanNoMigrate(Boolean.parseBoolean(System.getProperty(SystemProps.DB_CLEAN_NO_MIGRATE.getValue())));
        }
    }

    /**
     * Get the environment based on the 'spring.profiles.active' system property.
     *
     * @param value system property value
     * @return environment name
     */
    private String resolveEnvironmentFromSpringBootActiveProfiles(final String value) {
        if (value != null) {
            final List<String> profiles = Arrays.asList(value.split(","));

            if (profiles.size() >= 1) {
                return profiles.get(0);
            }
        }

        return null;
    }
}
