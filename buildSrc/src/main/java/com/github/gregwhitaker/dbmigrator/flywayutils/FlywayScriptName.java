package com.github.gregwhitaker.dbmigrator.flywayutils;

import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for creating and parsing database migration script names
 * that meet the platform naming conventions.
 *
 * Format: {type}{platform version}_{yyyyMMddhhmmss}__{description}.sql
 */
public class FlywayScriptName {

    /**
     * Type of database migration scripts.
     */
    public enum Type {
        VERSIONED("V"),
        UNDO("U"),
        REPEATABLE("R");

        private static final Map<String, Type> LOOKUP = new HashMap<>();

        static {
            EnumSet.allOf(Type.class).forEach(type -> LOOKUP.put(type.getCode(), type));
        }

        private final String code;

        Type(final String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }

        public static Type get(String code) {
            return LOOKUP.get(code.toUpperCase());
        }
    }

    private static final String DEFAULT_SCRIPT_NAME_FORMAT = "%s%s_%s__%s.sql";
    private static final String REPEATABLE_SCRIPT_NAME_FORMAT = "R__%s.sql";
    private static final String DEFAULT_DATE_FORMAT = "yyyyMMddhhmmss";

    /**
     * Generates a script name from the command line.
     *
     * @param args command line arguments
     */
    public static void main(String... args) {
        Args cliArgs = CommandLine.populateCommand(new Args(), args);

        if (!Arrays.asList("V", "U", "R").contains(cliArgs.type.toUpperCase())) {
            throw new IllegalArgumentException("Invalid migration script type specified: " + cliArgs.type);
        }

        System.out.println(FlywayScriptName.generate(Type.get(cliArgs.type), cliArgs.version, cliArgs.description));
    }

    private FlywayScriptName() {}

    /**
     * Generates a new "versioned" script name.
     *
     * @param version version number
     * @param description script description
     * @return formatted flyway migration script name
     */
    public static String generate(final String version, final String description) {
        return generate(Type.VERSIONED, version, description);
    }

    /**
     * Generates a new script name.
     *
     * @param type migration script type
     * @param version version number
     * @param description script description
     * @return formatted flyway migration script name
     */
    public static String generate(final Type type, final String version, final String description) {
        if (StringUtils.isEmpty(version)) {
            throw new IllegalArgumentException("Required argument missing or empty: version");
        }

        if (StringUtils.isEmpty(description)) {
            throw new IllegalArgumentException("Required argument missing or empty: description");
        }

        if (type == Type.REPEATABLE) {
            return String.format(REPEATABLE_SCRIPT_NAME_FORMAT, formatDescription(description));
        } else {
            return String.format(DEFAULT_SCRIPT_NAME_FORMAT,
                    type.getCode(),
                    formatVersion(version),
                    formattedDate(),
                    formatDescription(description));
        }
    }

    private static String formatVersion(String version) {
        assert StringUtils.isNotEmpty(version);

        version = version.toLowerCase();
        version = version.trim();

        return version.replaceAll("\\.", "_");
    }

    private static String formatDescription(String description) {
        assert StringUtils.isNotEmpty(description);

        description = description.toLowerCase();
        description = description.trim();

        return description.replaceAll(" ", "_");
    }

    private static String formattedDate() {
        final SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        return sdf.format(new Date());
    }

    /**
     * Command line arguments.
     */
    private static class Args {

        @CommandLine.Option(names = {"-t", "--type"}, arity = "1", defaultValue = "V", description = "V, U, R")
        private String type;

        @CommandLine.Option(names = {"-v", "--version"}, arity = "1", required = true, description = "version number")
        private String version;

        @CommandLine.Option(names = {"-d", "--desc"}, arity = "1", required = true, description = "migration description")
        private String description;
    }
}
