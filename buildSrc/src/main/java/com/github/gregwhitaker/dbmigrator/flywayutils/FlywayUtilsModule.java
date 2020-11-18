package com.github.gregwhitaker.dbmigrator.flywayutils;

import org.gradle.api.Project;

import java.util.HashMap;
import java.util.Map;

/**
 * Loads tasks and configuration for FlywayUtilsPlugin.
 */
public class FlywayUtilsModule {

    // Task Names
    public static final String CREATE_MIGRATION_SCRIPT_TASK_NAME = "createMigrationScript";
    public static final String CREATE_MIGRATION_CLASS_TASK_NAME = "createMigrationClass";

    /**
     * Loads and configures tasks for the plugin.
     *
     * @param project gradle project
     */
    public static void load(Project project) {
        final Map<String, Class> tasks = new HashMap<>();
        tasks.put(CREATE_MIGRATION_SCRIPT_TASK_NAME, CreateMigrationScript.class);
        tasks.put(CREATE_MIGRATION_CLASS_TASK_NAME, CreateMigrationClass.class);

        tasks.forEach((name, clazz) -> {
            // Register the default tasks with the project
            project.getTasks().create(name, clazz);
        });
    }
}
