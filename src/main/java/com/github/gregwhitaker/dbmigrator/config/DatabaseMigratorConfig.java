package com.github.gregwhitaker.dbmigrator.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for the database migrator.
 *
 * This configuration object first attempts to resolve configuration items from command line parameters, if not found
 * there it checks for environment variables.
 */
public final class DatabaseMigratorConfig {

    /**
     * Gets the database migrator configuration.
     *
     * @return database migrator configuration
     */
    public static DatabaseMigratorConfig get() {
        return DatabaseMigratorConfig.get(new String[]{});
    }

    /**
     * Gets the database migrator configuration.
     *
     * @param args command line arguments
     * @return database migrator configuration
     */
    public static DatabaseMigratorConfig get(String... args) {
        final DatabaseMigratorConfig config = new DatabaseMigratorConfig();

        // Define the config source hierarchy
        List<ConfigSource> configSources = new ArrayList<>();
        configSources.add(new EnvironmentConfigSource());
        configSources.add(new SystemPropertyConfigSource());
        configSources.add(new CommandLineConfigSource(args));

        // Run the configuration source builders
        configSources.forEach(configSource -> configSource.resolve(config));

        // Validate that all required fields have been configured
        config.validate();

        return config;
    }

    private String jdbcUrl;
    private String username;
    private String password;
    private String environment;
    private boolean cleanMigrate = false;
    private boolean cleanNoMigrate = false;

    private DatabaseMigratorConfig() {
        // Prevent direct instantiation
    }

    /**
     * Validates that all required fields are set on the configuration
     */
    private void validate() {
        if (getJdbcUrl() == null || getJdbcUrl().isEmpty()) {
            throw new MissingConfigurationException("jdbcUrl", getEnvironment());
        }
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    // Package scoped so config source implementations in chain have access, but
    // object is immutable once returned from builder
    void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    // Package scoped so config source implementations in chain have access, but
    // object is immutable once returned from builder
    void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    // Package scoped so config source implementations in chain have access, but
    // object is immutable once returned from builder
    void setPassword(String password) {
        this.password = password;
    }

    public String getEnvironment() {
        return environment;
    }

    // Package scoped so config source implementations in chain have access, but
    // object is immutable once returned from builder
    void setEnvironment(String environment) {
        this.environment = environment;
    }

    public boolean isCleanMigrate() {
        return cleanMigrate;
    }

    // Package scoped so config source implementations in chain have access, but
    // object is immutable once returned from builder
    void setCleanMigrate(boolean cleanMigrate) {
        this.cleanMigrate = cleanMigrate;
    }

    public boolean isCleanNoMigrate() {
        return cleanNoMigrate;
    }

    // Package scoped so config source implementations in chain have access, but
    // object is immutable once returned from builder
    void setCleanNoMigrate(boolean cleanNoMigrate) {
        this.cleanNoMigrate = cleanNoMigrate;
    }
}
