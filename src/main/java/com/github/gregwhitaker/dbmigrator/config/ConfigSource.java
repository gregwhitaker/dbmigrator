package com.github.gregwhitaker.dbmigrator.config;

/**
 * Interface that all configuration sources must implement.
 */
public interface ConfigSource {

    /**
     * Resolves properties, if they exist, to a {@link DatabaseMigratorConfig} instance.
     *
     * @param config database migrator config
     */
    void resolve(DatabaseMigratorConfig config);
}
