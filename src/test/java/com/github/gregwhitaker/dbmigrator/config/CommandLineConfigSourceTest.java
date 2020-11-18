package com.github.gregwhitaker.dbmigrator.config;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class CommandLineConfigSourceTest {

    @Test
    public void shouldSetConfigurationFromCommandLineArguments() {
        final String[] args = {
                "--jdbc-url", "db-jdbc-url",
                "--username", "db-username",
                "--password", "db-password",
                "--env", "db-env",
        };

        DatabaseMigratorConfig config = DatabaseMigratorConfig.get(args);

        assertEquals(config.getJdbcUrl(), "db-jdbc-url");
        assertEquals(config.getUsername(), "db-username");
        assertEquals(config.getPassword(), "db-password");
        assertEquals(config.getEnvironment(), "db-env");
        assertFalse(config.isCleanMigrate());
        assertFalse(config.isCleanNoMigrate());
    }

    @Test
    public void shouldSetCleanMigration() {
        final String[] args = {
                "--jdbc-url", "db-jdbc-url",
                "--username", "db-username",
                "--password", "db-password",
                "--env", "db-env",
                "--clean-migrate"
        };

        DatabaseMigratorConfig config = DatabaseMigratorConfig.get(args);

        assertEquals(config.getJdbcUrl(), "db-jdbc-url");
        assertEquals(config.getUsername(), "db-username");
        assertEquals(config.getPassword(), "db-password");
        assertEquals(config.getEnvironment(), "db-env");
        assertTrue(config.isCleanMigrate());
        assertFalse(config.isCleanNoMigrate());
    }

    @Test
    public void shouldSetCleanNoMigrate() {
        final String[] args = {
                "--jdbc-url", "db-jdbc-url",
                "--username", "db-username",
                "--password", "db-password",
                "--env", "db-env",
                "--clean-no-migrate"
        };

        DatabaseMigratorConfig config = DatabaseMigratorConfig.get(args);

        assertEquals(config.getJdbcUrl(), "db-jdbc-url");
        assertEquals(config.getUsername(), "db-username");
        assertEquals(config.getPassword(), "db-password");
        assertEquals(config.getEnvironment(), "db-env");
        assertFalse(config.isCleanMigrate());
        assertTrue(config.isCleanNoMigrate());
    }

    @Test(expected = Exception.class)
    public void shouldThrowExceptionIfBothCleanFlagsSupplied() {
        final String[] args = {
                "--jdbc-url", "db-jdbc-url",
                "--username", "db-username",
                "--password", "db-password",
                "--env", "db-env",
                "--clean-migrate",
                "--clean-no-migrate",
        };

        DatabaseMigratorConfig config = DatabaseMigratorConfig.get(args);
    }

    @Test(expected = MissingConfigurationException.class)
    public void shouldThrowExceptionIfJdbcUrlNotSupplied() {
        final String[] args = {
                "--username", "db-username",
                "--password", "db-password",
                "--env", "db-env",
        };

        DatabaseMigratorConfig config = DatabaseMigratorConfig.get(args);
    }
}
