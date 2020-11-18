package com.github.gregwhitaker.dbmigrator.config;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestRule;

import static org.junit.Assert.*;

public class DatabaseMigratorConfigTest {

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Rule
    public final ProvideSystemProperty dbJdbcUrlSystemProp = new ProvideSystemProperty("db.jdbcUrl", "db-jdbc-url-sp");

    @Rule
    public final ProvideSystemProperty dbUsernameSystemProp = new ProvideSystemProperty("db.username", "db-username-sp");

    @Rule
    public final ProvideSystemProperty dbPasswordSystemProp = new ProvideSystemProperty("db.password", "db-password-sp");

    @Rule
    public final ProvideSystemProperty dbEnvSystemProp = new ProvideSystemProperty("db.env", "db-env-sp");

    @Rule
    public final ProvideSystemProperty dbCleanMigrateSystemProp = new ProvideSystemProperty("db.cleanMigrate", "false");

    @Rule
    public final ProvideSystemProperty dbCleanNoMigrateSystemProp = new ProvideSystemProperty("db.cleanNoMigrate", "true");

    @Rule
    public final TestRule restoreSystemProperties = new RestoreSystemProperties();

    @Test
    public void shouldOverwriteEnvironmentVariablesWithSystemProperties() {
        environmentVariables.set("DB_JDBC_URL", "db-jdbc-url");
        environmentVariables.set("DB_USERNAME", "db-username");
        environmentVariables.set("DB_PASSWORD", "db-password");
        environmentVariables.set("DB_ENV", "db-env");
        environmentVariables.set("DB_CLEAN_MIGRATE", "true");
        environmentVariables.set("DB_CLEAN_NO_MIGRATE", "false");

        final DatabaseMigratorConfig config = DatabaseMigratorConfig.get();

        assertEquals(config.getJdbcUrl(), "db-jdbc-url-sp");
        assertEquals(config.getUsername(), "db-username-sp");
        assertEquals(config.getPassword(), "db-password-sp");
        assertEquals(config.getEnvironment(), "db-env-sp");
        assertFalse(config.isCleanMigrate());
        assertTrue(config.isCleanNoMigrate());
    }

    @Test
    public void shouldOverwriteSystemPropertiesWithCommandLineArguments() {
        final String[] args = new String[]{
                "--jdbc-url", "db-jdbc-url-cl",
                "--username", "db-username-cl",
                "--password", "db-password-cl",
                "--env", "db-env-cl",
                "--clean-migrate"
        };

        final DatabaseMigratorConfig config = DatabaseMigratorConfig.get(args);

        assertEquals(config.getJdbcUrl(), "db-jdbc-url-cl");
        assertEquals(config.getUsername(), "db-username-cl");
        assertEquals(config.getPassword(), "db-password-cl");
        assertEquals(config.getEnvironment(), "db-env-cl");
        assertTrue(config.isCleanMigrate());
        assertFalse(config.isCleanNoMigrate());
    }

    @Test(expected = MissingConfigurationException.class)
    public void shouldThrowExceptionIfNoJdbcUrlConfigured() {
        System.clearProperty("db.jdbcUrl");

        final String[] args = new String[]{
                "--username", "db-username-cl",
                "--password", "db-password-cl",
                "--env", "db-env-cl",
                "--clean-migrate"
        };

        final DatabaseMigratorConfig config = DatabaseMigratorConfig.get(args);
    }
}
