package com.github.gregwhitaker.dbmigrator.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration source that retrieves values from environment variables.
 */
public class EnvironmentConfigSource implements ConfigSource {
    private static final Logger LOG = LoggerFactory.getLogger(EnvironmentConfigSource.class);

    /**
     * Enumeration of configuration environment variables.
     */
    public enum EnvironmentVars {
        DB_JDBC_URL("DB_JDBC_URL"),
        DB_USERNAME("DB_USERNAME"),
        DB_PASSWORD("DB_PASSWORD"),
        DB_ENV("DB_ENV"),
        DB_CLEAN_MIGRATE("DB_CLEAN_MIGRATE"),
        DB_CLEAN_NO_MIGRATE("DB_CLEAN_NO_MIGRATE");

        private final String value;

        EnvironmentVars(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    public void resolve(final DatabaseMigratorConfig config) {
        LOG.debug("Resolving configuration properties via environment variables");

        if (System.getenv(EnvironmentVars.DB_JDBC_URL.getValue()) != null) {
            config.setJdbcUrl(System.getenv(EnvironmentVars.DB_JDBC_URL.getValue()));
        }

        if (System.getenv(EnvironmentVars.DB_USERNAME.getValue()) != null) {
            config.setUsername(System.getenv(EnvironmentVars.DB_USERNAME.getValue()));
        }

        if (System.getenv(EnvironmentVars.DB_PASSWORD.getValue()) != null) {
            config.setPassword(System.getenv(EnvironmentVars.DB_PASSWORD.getValue()));
        }

        if (System.getenv(EnvironmentVars.DB_ENV.getValue()) != null) {
            config.setEnvironment(System.getenv(EnvironmentVars.DB_ENV.getValue()));
        }

        if (System.getenv(EnvironmentVars.DB_CLEAN_MIGRATE.getValue()) != null) {
            config.setCleanMigrate(Boolean.parseBoolean(System.getenv(EnvironmentVars.DB_CLEAN_MIGRATE.getValue())));
        }

        if (System.getenv(EnvironmentVars.DB_CLEAN_NO_MIGRATE.getValue()) != null) {
            config.setCleanNoMigrate(Boolean.parseBoolean(System.getenv(EnvironmentVars.DB_CLEAN_NO_MIGRATE.getValue())));
        }
    }
}
