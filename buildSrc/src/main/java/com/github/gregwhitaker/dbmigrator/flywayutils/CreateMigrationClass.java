package com.github.gregwhitaker.dbmigrator.flywayutils;

import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.commons.lang3.StringUtils;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Task that creates a new Flyway migration Java class in the project with standard naming convention.
 */
public class CreateMigrationClass extends DefaultTask {
    private static final List<String> VALID_MIGRATION_TYPES = Arrays.asList("V", "U", "R");

    @Input
    private String type;

    @Input
    private String ver;

    @Input
    private String desc;

    @Optional
    @Input
    private String env;

    @TaskAction
    public void run() {
        validateType(type);
        validateVersion(ver);
        validateDescription(desc);
        validateEnv(env);

        String className = FlywayScriptName.generate(FlywayScriptName.Type.get(type), ver, desc);
        className = className.substring(0, className.lastIndexOf("."));

        JavaFile javaFile = createJavaMigrationClass(className);

        Path filePath;
        filePath = Paths.get(getProject().getRootProject().getProjectDir().getAbsolutePath(), "/src/main/java");

        try {
            Path path = javaFile.writeToPath(filePath);
            System.out.println("Created migration class: " + path.toString());
        } catch (IOException e) {
            throw new GradleException("Error occurred while creating migration script", e);
        }
    }

    private void validateType(String type) {
        // Required argument
        if (StringUtils.isBlank(type)) {
            throw new GradleException("Parameter 'type' is required.");
        }

        // Value must be a supported migration type
        if (!VALID_MIGRATION_TYPES.contains(type.toUpperCase())) {
            throw new GradleException("Invalid 'type' parameter value. Must be either 'V', 'U', or 'R'.");
        }
    }

    private void validateVersion(String version) {
        // Required argument
        if (StringUtils.isBlank(version)) {
            throw new GradleException("Parameter 'ver' is required.");
        }
    }

    private void validateDescription(String description) {
        // Required argument
        if (StringUtils.isBlank(description)) {
            throw new GradleException("Parameter 'desc' is required.");
        }
    }

    private void validateEnv(String env) {
        // Optional argument that when supplied must be a valid environment name in the `migration-env` directory
        if (StringUtils.isNotBlank(env)) {
            Path migrationEnvPath = Paths.get(getProject().getRootProject().getProjectDir().getAbsolutePath(), "/src/main/resources/db/migration-env");
            File[] envDirs = migrationEnvPath.toFile().listFiles(File::isDirectory);

            if (envDirs != null) {
                if (Arrays.stream(envDirs).noneMatch(file -> file.getName().equalsIgnoreCase(env))) {
                    throw new GradleException(String.format("No environment directory for environment '%s' exists in the 'migration-env' directory.", env));
                }
            } else {
                throw new GradleException("No environment directory exist in 'migration-env' directory.");
            }
        }
    }

    private JavaFile createJavaMigrationClass(String className) {
        MethodSpec methodSpec = MethodSpec.methodBuilder("migrate")
                .addException(Exception.class)
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .addParameter(Context.class, "context")
                .addComment("Implement the migration here")
                .build();

        FieldSpec loggerFieldSpec = FieldSpec.builder(Logger.class, "LOG", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer(String.format("$T.getLogger(%s.class)", className), ParameterizedTypeName.get(LoggerFactory.class))
                .build();

        TypeSpec typeSpec = TypeSpec.classBuilder(className)
                .addModifiers(Modifier.PUBLIC)
                .superclass(BaseJavaMigration.class)
                .addField(loggerFieldSpec)
                .addMethod(methodSpec)
                .build();

        return JavaFile.builder("db.migration", typeSpec)
                .build();
    }

    @Override
    public String getGroup() {
        return FlywayUtilsPlugin.GROUP_NAME;
    }

    @Override
    public String getDescription() {
        return "Creates a new Flyway migration Java class with standard naming convention.";
    }

    public String getType() {
        return type;
    }

    @Option(option = "type", description = "Migration type (V,R,U)")
    public void setType(String type) {
        this.type = type;
    }

    public String getVersion() {
        return ver;
    }

    @Option(option = "ver", description = "Migration version")
    public void setVersion(String ver) {
        this.ver = ver;
    }

    public String getDesc() {
        return desc;
    }

    @Option(option = "desc", description = "Migration description")
    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getEnv() {
        return env;
    }

    @Option(option = "env", description = "Migration environment (defaults to all environments)")
    public void setEnv(String env) {
        this.env = env;
    }
}
