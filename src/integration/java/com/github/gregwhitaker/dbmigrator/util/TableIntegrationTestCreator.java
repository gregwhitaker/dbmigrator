package com.github.gregwhitaker.dbmigrator.util;

import com.github.gregwhitaker.dbmigrator.table.BaseTableIntegrationTest;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.text.WordUtils;
import picocli.CommandLine;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility that creates database table integration tests.
 */
public class TableIntegrationTestCreator {
    private static final String TABLE_INTEG_TEST_PACKAGE = "com.github.gregwhitaker.dbmigrator.table";

    /**
     * Main entry-point of the application.
     *
     * @param args command line arguments
     */
    public static void main(String... args) throws Exception {
        final CommandLineArgs commandLineArgs = CommandLine.populateCommand(new CommandLineArgs(), args);

        final TableIntegrationTestCreator tableIntegTestBuilder = new TableIntegrationTestCreator();
        tableIntegTestBuilder.newTableTest(commandLineArgs.projectDir,
                commandLineArgs.tableName,
                commandLineArgs.force);
    }

    //
    // API
    //

    /**
     * Creates a new table integration test class.
     *
     * @param projectDir directory path of the project
     * @param tableName name of table for which to create an integration test class
     * @param force overwrite existing integration test
     * @throws Exception
     */
    public void newTableTest(String projectDir, String tableName, boolean force) throws Exception {
        if (!isValidTable(tableName)) {
            throw new RuntimeException("Table does not exist: " + tableName);
        }

        // Database Info
        final Map<String, ColumnInformation> tableColumns = getTableColumns(tableName);

        // Class Generation Info
        final String clazzName = generateTestClassName(tableName);

        // Generate class
        AnnotationSpec databaseTableTestAnnotation = AnnotationSpec.builder(DatabaseTableTest.class)
                .addMember("tableName", "$S", tableName)
                .build();

        FieldSpec staticNameField = FieldSpec.builder(String.class, "TABLE_NAME")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .initializer("$S", tableName)
                .build();

        ClassName map = ClassName.get("java.util", "Map");
        TypeName string = ClassName.get(String.class);
        ClassName expectedColumnInfo = ClassName.get(BaseTableIntegrationTest.ExpectedColumnInformation.class);
        TypeName mapOfExpectedColumns = ParameterizedTypeName.get(map, string, expectedColumnInfo);
        FieldSpec staticExpectedColumnsField = FieldSpec.builder(mapOfExpectedColumns, "EXPECTED_COLUMNS")
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer("new $T<>()", LinkedHashMap.class)
                .build();

        CodeBlock.Builder staticBlockBuilder = CodeBlock.builder();
        tableColumns.entrySet().stream()
                .sorted(Comparator.comparingInt(o -> o.getValue().getOrdinalPosition()))
                .forEach(entry -> {
                    if (entry.getValue().getColumnDefault() == null) {
                        staticBlockBuilder.add("$L.put($S, new ExpectedColumnInformation($S, $L));\n", "EXPECTED_COLUMNS",
                                entry.getKey(), entry.getValue().getUdtName(), entry.getValue().isNullable);
                    } else {
                        staticBlockBuilder.add("$L.put($S, new ExpectedColumnInformation($S, $L, $S));\n", "EXPECTED_COLUMNS",
                                entry.getKey(), entry.getValue().getUdtName(), entry.getValue().isNullable, entry.getValue().columnDefault);
                    }
                });

        MethodSpec defaultConstructor = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addStatement("super($L, $L)", "TABLE_NAME", "EXPECTED_COLUMNS")
                .build();

        TypeSpec typeSpec = TypeSpec.classBuilder(clazzName)
                .addJavadoc("Tests for the $S table.", tableName)
                .addModifiers(Modifier.PUBLIC)
                .superclass(BaseTableIntegrationTest.class)
                .addAnnotation(databaseTableTestAnnotation)
                .addField(staticNameField)
                .addField(staticExpectedColumnsField)
                .addStaticBlock(staticBlockBuilder.build())
                .addMethod(defaultConstructor)
                .build();

        JavaFile javaFile = JavaFile.builder(TABLE_INTEG_TEST_PACKAGE, typeSpec)
                .build();

        // Don't create the test file if it already exists, unless the "--force" option is used.
        File destFile = Paths.get(projectDir, "src/integration/java/com/github/gregwhitaker/dbmigraator/table", clazzName + ".java").toFile();
        if (destFile.exists() && !force) {
            throw new IOException("File already exists: " + destFile.getAbsolutePath());
        }

        Path path = javaFile.writeToPath(Paths.get(projectDir, "src/integration/java"));

        System.out.println("Created: " + path.toString());
    }

    //
    // Helpers
    //

    /**
     * Generates a table integration test class name based on the database table name.
     *
     * @param tableName database table name
     * @return class name for table integration test
     */
    private String generateTestClassName(final String tableName) {
        return String.format("%sTableIntegrationTest", WordUtils.capitalizeFully(tableName, new char[]{'_'}).replaceAll("_", ""));
    }

    /**
     * Checks to see if the supplied tableName is a valid database table.
     *
     * @param tableName name of table to check
     * @return <code>true</code> if the table exists; otherwise <code>false</code>
     * @throws SQLException
     */
    private boolean isValidTable(final String tableName) throws SQLException {
        try (Connection conn = DataSourceHelper.getInstance().getDataSource().getConnection()) {
            final String sql = String.format("SELECT * " +
                    "FROM   information_schema.tables " +
                    "WHERE  table_schema = '%s' " +
                    "AND    table_name = ?"
            , DataSourceHelper.DEFAULT_SCHEMA);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, tableName);

                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        }
    }

    /**
     * Gets all column information for the table.
     *
     * @return map of column name to column information
     * @throws SQLException
     */
    private Map<String, ColumnInformation> getTableColumns(final String tableName) throws SQLException {
        final Map<String, ColumnInformation> columnInfo = new HashMap<>();

        try (Connection conn = DataSourceHelper.getInstance().getDataSource().getConnection()) {
            final String sql = "SELECT * " +
                            "FROM   information_schema.columns " +
                            "WHERE  table_schema = ? " +
                            "AND    table_name = ?";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, DataSourceHelper.DEFAULT_SCHEMA);
                ps.setString(2, tableName);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        columnInfo.put(rs.getString("column_name"),
                                new ColumnInformation(
                                        rs.getString("table_schema"),
                                        rs.getString("table_name"),
                                        rs.getString("column_name"),
                                        rs.getInt("ordinal_position"),
                                        rs.getString("column_default"),
                                        rs.getString("is_nullable").equalsIgnoreCase("YES"),
                                        rs.getString("data_type"),
                                        rs.getLong("character_maximum_length"),
                                        rs.getString("udt_name")
                                )
                        );
                    }
                }
            }
        }

        return columnInfo;
    }

    //
    // Nested Classes
    //

    /**
     * Command line arguments.
     */
    public static class CommandLineArgs {
        @CommandLine.Option(names = {"--project-dir"}, required = true, description = "Fully-qualified project directory path.")
        public String projectDir;

        @CommandLine.Option(names = {"--table-name"}, required = true, description = "Name of database table for which to generate an integration test.")
        public String tableName;

        @CommandLine.Option(names = { "--force" }, defaultValue = "false", description = "Force file generation if test already exists.")
        public boolean force;
    }

    /**
     * Table column information.
     */
    @Data
    @AllArgsConstructor
    static class ColumnInformation {
        private String tableSchema;
        private String tableName;
        private String columnName;
        private Integer ordinalPosition;
        private String columnDefault;
        private Boolean isNullable;
        private String dataType;
        private Long characterMaxLength;
        private String udtName;
    }
}
