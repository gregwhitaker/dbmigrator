package com.github.gregwhitaker.dbmigrator;

import com.github.gregwhitaker.dbmigrator.config.DatabaseMigratorConfig;
import com.github.gregwhitaker.envopts.EnvOpts;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * Manages the migration of database entities.
 */
public class DatabaseMigrator {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseMigrator.class);

    /**
     * Runs a database migration from the command line.
     *
     * @param args command line arguments
     */
    public static void main(String... args) {
        EnvOpts.parse();

        final DatabaseMigratorConfig config = DatabaseMigratorConfig.get(args);

        LOG.info("Connecting to database [env: '{}', jdbcUrl: '{}', username: '{}']",
                config.getEnvironment(),
                config.getJdbcUrl(),
                config.getUsername());

        // Configure Datasource
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getJdbcUrl());
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setInitializationFailTimeout(30_000);

        if (config.getPassword() != null) {
            hikariConfig.setPassword(config.getPassword());
        }

        // Start Migration
        DatabaseMigrator migrator = new DatabaseMigrator(new HikariDataSource(hikariConfig));

        if (config.isCleanNoMigrate()) {
            // No migration, just clean the database
            migrator.clean();
        } else {
            // Run the migration
            migrator.run(config.getEnvironment(), config.isCleanMigrate());
        }
    }

    private final DataSource dataSource;

    /**
     * Creates a new instance of {@link DatabaseMigrator}.
     *
     * @param dataSource datasource to use for migration
     */
    public DatabaseMigrator(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Run database migration.
     *
     * @param env migration environment name or <code>null</code> if no environment is desired
     * @param cleanMigration flag indicating whether or not to clean the database before running the migration
     */
    public void run(final String env, boolean cleanMigration) {
        String[] locations;
        if (env == null || env.isEmpty()) {
            // No environment specified so just run the standard migration
            locations = new String[]{"classpath:/db/migration"};
        } else {
            locations = new String[]{"classpath:/db/migration", "classpath:/db/migration-env/" + env.toLowerCase()};
        }

        LOG.info("Running database migrator... [env: '{}', cleanMigration: '{}', locations: '{}']",
                env, cleanMigration, String.join(",", locations));

        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .baselineOnMigrate(false)
                .locations(locations)
                .load();

        if (cleanMigration) {
            flyway.clean();
        }

        flyway.migrate();
    }

    /**
     * Cleans the database.
     */
    public void clean() {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .outOfOrder(true)
                .baselineOnMigrate(false)
                .load();

        flyway.clean();
    }
}
