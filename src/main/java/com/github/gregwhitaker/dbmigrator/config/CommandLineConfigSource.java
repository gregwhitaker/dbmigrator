package com.github.gregwhitaker.dbmigrator.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Configuration source that retrieves configuration from command line parameters.
 */
public class CommandLineConfigSource implements ConfigSource {
    private static final Logger LOG = LoggerFactory.getLogger(CommandLineConfigSource.class);

    private final String[] args;

    public CommandLineConfigSource(String... args) {
        this.args = args;
    }

    @Override
    public void resolve(DatabaseMigratorConfig config) {
        if (args != null) {
            LOG.debug("Resolving configuration properties via command line arguments");

            // Parse Command-Line Arguments
            CommandLineArgs parsedConfig = CommandLine.populateCommand(new CommandLineArgs(), args);

            if (parsedConfig.jdbcUrl != null && !parsedConfig.jdbcUrl.isEmpty()) {
                config.setJdbcUrl(parsedConfig.jdbcUrl);
            }

            if (parsedConfig.username != null && !parsedConfig.username.isEmpty()) {
                config.setUsername(parsedConfig.username);
            }

            if (parsedConfig.password != null && !parsedConfig.password.isEmpty()) {
                config.setPassword(parsedConfig.password);
            }

            if (parsedConfig.env != null && !parsedConfig.env.isEmpty()) {
                config.setEnvironment(parsedConfig.env);
            }

            if (parsedConfig.cleanAndMigrateArgs != null) {
                config.setCleanMigrate(parsedConfig.cleanAndMigrateArgs.cleanMigrate);
                config.setCleanNoMigrate(parsedConfig.cleanAndMigrateArgs.cleanNoMigrate);
            }
        }
    }

    /**
     * DatabaseMigrator command line arguments.
     */
    public static class CommandLineArgs {

        @CommandLine.Option(names = { "--jdbc-url" }, description = "Database jdbc connection url")
        public String jdbcUrl;

        @CommandLine.Option(names = { "--username" }, description = "Database username")
        public String username;

        @CommandLine.Option(names = { "--password" }, description = "Database password")
        public String password;

        @CommandLine.Option(names = { "--env" }, description = "Migration environment name")
        public String env;

        @CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
        CleanAndMigrateArgs cleanAndMigrateArgs;

        static class CleanAndMigrateArgs {
            @CommandLine.Option(names = { "--clean-migrate" }, defaultValue = "false", description = "Run clean before migration")
            public boolean cleanMigrate;

            @CommandLine.Option(names = { "--clean-no-migrate" }, defaultValue = "false", description = "Run database clean without migration")
            public boolean cleanNoMigrate;
        }
    }
}
