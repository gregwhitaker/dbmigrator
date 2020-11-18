package com.github.gregwhitaker.dbmigrator.config;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestRule;

import static org.junit.Assert.*;

public class SystemPropertyConfigSourceTest {

    @Rule
    public final ProvideSystemProperty dbJdbcUrlSystemProp = new ProvideSystemProperty("db.jdbcUrl", "db-jdbc-url");

    @Rule
    public final ProvideSystemProperty dbUsernameSystemProp = new ProvideSystemProperty("db.username", "db-username");

    @Rule
    public final ProvideSystemProperty dbPasswordSystemProp = new ProvideSystemProperty("db.password", "db-password");

    @Rule
    public final ProvideSystemProperty dbEnvSystemProp = new ProvideSystemProperty("db.env", "db-env");

    @Rule
    public final ProvideSystemProperty dbEnvSpringSystemProp = new ProvideSystemProperty("spring.profiles.active", "db-env-spring");

    @Rule
    public final ProvideSystemProperty dbCleanMigrateSystemProp = new ProvideSystemProperty("db.cleanMigrate", "true");

    @Rule
    public final ProvideSystemProperty dbCleanNoMigrateSystemProp = new ProvideSystemProperty("db.cleanNoMigrate", "false");

    @Rule
    public final TestRule restoreSystemProperties = new RestoreSystemProperties();

    @Test
    public void shouldSetConfigurationFromSystemProperties() {
        System.clearProperty("spring.profiles.active");

        final DatabaseMigratorConfig config = DatabaseMigratorConfig.get();

        assertEquals(config.getJdbcUrl(), "db-jdbc-url");
        assertEquals(config.getUsername(), "db-username");
        assertEquals(config.getPassword(), "db-password");
        assertEquals(config.getEnvironment(), "db-env");
        assertTrue(config.isCleanMigrate());
        assertFalse(config.isCleanNoMigrate());
    }

    @Test
    public void shouldSetEnvironmentFromSpringBootActiveProfilesProperty() {
        System.clearProperty("db.env");

        final DatabaseMigratorConfig config = DatabaseMigratorConfig.get();

        assertEquals(config.getJdbcUrl(), "db-jdbc-url");
        assertEquals(config.getUsername(), "db-username");
        assertEquals(config.getPassword(), "db-password");
        assertEquals(config.getEnvironment(), "db-env-spring");
        assertTrue(config.isCleanMigrate());
        assertFalse(config.isCleanNoMigrate());
    }

    @Test(expected = MissingConfigurationException.class)
    public void shouldThrowExceptionIfJdbcUrlNotSupplied() {
        System.clearProperty("db.jdbcUrl");

        DatabaseMigratorConfig.get();
    }
}
