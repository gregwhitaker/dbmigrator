package com.github.gregwhitaker.dbmigrator.flywayutils;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * Plugin that adds helpful utilities for working with Flyway database migrations.
 */
public class FlywayUtilsPlugin implements Plugin<Project> {
    public static final String GROUP_NAME = "Database Migration";

    @Override
    public void apply(Project project) {
        loadModules(project);
    }

    /**
     * Loads the modules containing tasks and configuration for this plugin.
     *
     * @param project gradle project
     */
    private void loadModules(Project project) {
        FlywayUtilsModule.load(project);
    }
}
