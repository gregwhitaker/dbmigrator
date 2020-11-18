package com.github.gregwhitaker.dbmigrator.config;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import static org.junit.Assert.*;

public class EnvironmentConfigSourceTest {

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test
    public void shouldSetConfigurationFromEnvVariables() {
        environmentVariables.set("DB_JDBC_URL", "db-jdbc-url");
        environmentVariables.set("DB_USERNAME", "db-username");
        environmentVariables.set("DB_PASSWORD", "db-password");
        environmentVariables.set("DB_ENV", "db-env");
        environmentVariables.set("DB_CLEAN_MIGRATE", "true");
        environmentVariables.set("DB_CLEAN_NO_MIGRATE", "false");

        final DatabaseMigratorConfig config = DatabaseMigratorConfig.get();

        assertEquals(config.getJdbcUrl(), "db-jdbc-url");
        assertEquals(config.getUsername(), "db-username");
        assertEquals(config.getPassword(), "db-password");
        assertEquals(config.getEnvironment(), "db-env");
        assertTrue(config.isCleanMigrate());
        assertFalse(config.isCleanNoMigrate());
    }

    @Test(expected = MissingConfigurationException.class)
    public void shouldThrowExceptionIfJdbcUrlNotSupplied() {
        environmentVariables.set("DB_USERNAME", "db-username");
        environmentVariables.set("DB_PASSWORD", "db-password");
        environmentVariables.set("DB_ENV", "db-env");

        DatabaseMigratorConfig.get();
    }
}
