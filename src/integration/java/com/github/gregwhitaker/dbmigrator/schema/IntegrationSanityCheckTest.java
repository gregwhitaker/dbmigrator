package com.github.gregwhitaker.dbmigrator.schema;

import com.github.gregwhitaker.dbmigrator.IntegrationTestSuite;
import com.github.gregwhitaker.dbmigrator.util.DataSourceHelper;
import com.github.gregwhitaker.dbmigrator.util.DatabaseTableTest;
import org.junit.Test;
import org.junit.runners.Suite;
import org.reflections.Reflections;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that verify the validity of the integration tests.
 */
public class IntegrationSanityCheckTest {

    /**
     * Tables that are excluded from sanity checking
     */
    private static final List<String> EXCLUDED_TABLE_NAMES = Arrays.asList(
            "flyway_schema_history"
    );

    @Test
    public void shouldHaveIntegrationTestForAllTables() throws SQLException {
        final List<String> expectedTableNames = getTableNamesFromDb();
        final List<String> foundTableNames = getTableNamesFromIntegrationTests();

        final String missingTables = expectedTableNames.stream()
                .filter(expectedTableName -> !foundTableNames.contains(expectedTableName))
                .collect(Collectors.joining(","));

        assertTrue(String.format("Missing integration tests for tables: [%s]", missingTables), foundTableNames.containsAll(expectedTableNames));
        assertEquals(expectedTableNames.size(), foundTableNames.size());
    }

    @Test
    public void shouldHaveIntegrationTestsRegisteredInIntegTestSuite() {
        final List<String> expectedIntegrationTestTables = getTableNamesFromIntegrationTests();
        final List<String> registeredIntegrationTestTables = new ArrayList<>();

        // Get all table integration tests registered in the IntegrationTestSuite
        Suite.SuiteClasses suiteClassesAnnotation = IntegrationTestSuite.class.getAnnotation(Suite.SuiteClasses.class);
        Arrays.stream(suiteClassesAnnotation.value())
                .forEach(integTestClazz -> {
                    DatabaseTableTest databaseTableTestAnnotation = integTestClazz.getAnnotation(DatabaseTableTest.class);
                    if (databaseTableTestAnnotation != null) {
                        registeredIntegrationTestTables.add(databaseTableTestAnnotation.tableName());
                    }
                });

        final String missingTables = expectedIntegrationTestTables.stream()
                .filter(expectedTableName -> !registeredIntegrationTestTables.contains(expectedTableName))
                .collect(Collectors.joining(","));

        assertTrue(String.format("Found integration tests not registered in IntegrationTestSuite for tables: [%s]", missingTables), registeredIntegrationTestTables.containsAll(expectedIntegrationTestTables));
        assertEquals(registeredIntegrationTestTables.size(), expectedIntegrationTestTables.size());
    }

    /**
     * Gets a list of the names of all tables in the database.
     *
     * @return a list of table names
     * @throws SQLException
     */
    private List<String> getTableNamesFromDb() throws SQLException {
        final List<String> tableNames = new ArrayList<>();
        try (Connection conn = DataSourceHelper.getInstance().getDataSource().getConnection()) {
            final String sql = String.format("SELECT table_name " +
                    "FROM   information_schema.tables " +
                    "WHERE  table_schema = '%s'", DataSourceHelper.DEFAULT_SCHEMA);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        tableNames.add(rs.getString("table_name"));
                    }
                }
            }
        }

        // Filter out any excluded table names from the results before returning
        return tableNames.stream()
                .filter(s -> !EXCLUDED_TABLE_NAMES.contains(s.toLowerCase()))
                .collect(Collectors.toList());
    }

    /**
     * Gets the table names of all table integration test classes annotated with {@link DatabaseTableTest}.
     *
     * @return a list of database table names
     */
    private List<String> getTableNamesFromIntegrationTests() {
        final List<String> tableNames = new ArrayList<>();

        final Reflections reflections = new Reflections("com.github.gregwhitaker.dbmigrator.table");
        reflections.getTypesAnnotatedWith(DatabaseTableTest.class).forEach(clazz -> {
            DatabaseTableTest annotation = clazz.getAnnotation(DatabaseTableTest.class);
            tableNames.add(annotation.tableName());
        });

        return tableNames;
    }
}
