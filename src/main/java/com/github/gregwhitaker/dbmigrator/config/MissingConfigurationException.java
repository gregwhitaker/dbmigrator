package com.github.gregwhitaker.dbmigrator.config;

/**
 * Exception thrown when a required configuration option is missing.
 */
public class MissingConfigurationException extends IllegalArgumentException {

    private final String configOption;

    public MissingConfigurationException(String configOption, String environment) {
        super(String.format("Missing configuration option in environment. Please configure before restarting the migration. [config: '%s', environment: '%s']", configOption, environment));
        this.configOption = configOption;
    }

    /**
     * Gets the missing configuration option.
     *
     * @return name of missing configuration option
     */
    public String getConfigOption() {
        return configOption;
    }
}
