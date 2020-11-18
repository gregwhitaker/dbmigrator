package com.github.gregwhitaker.dbmigrator.schema;

import com.github.gregwhitaker.dbmigrator.util.DataSourceHelper;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the entire database schema.
 */
public class SchemaIntegrationTest {

    /**
     * List of all expected table names in the database schema.
     */
    private static final List<String> EXPECTED_TABLE_NAMES = Arrays.asList(
            "flyway_schema_history"
    );

    @Test
    public void shouldHaveSuccessfullyExecutedAllMigrations() throws SQLException {
        try (Connection conn = DataSourceHelper.getInstance().getDataSource().getConnection()) {
            final String sql = "SELECT COUNT(*) AS failed_migrations " +
                    "FROM   flyway_schema_history " +
                    "WHERE  success != 1";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(0, rs.getInt("failed_migrations"));
                }
            }
        }
    }

    @Test
    public void shouldHaveCorrectNumberOfTables() throws SQLException {
        try (Connection conn = DataSourceHelper.getInstance().getDataSource().getConnection()) {
            final String sql = String.format("SELECT COUNT(*) AS num_tables " +
                    "FROM   information_schema.tables " +
                    "WHERE  table_schema = '%s'", DataSourceHelper.DEFAULT_SCHEMA);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(EXPECTED_TABLE_NAMES.size(), rs.getInt("num_tables"));
                }
            }
        }
    }

    @Test
    public void shouldHaveCorrectTableNames() throws SQLException {
        try (Connection conn = DataSourceHelper.getInstance().getDataSource().getConnection()) {
            final String sql = String.format("SELECT table_name " +
                    "FROM   information_schema.tables " +
                    "WHERE  table_schema = '%s'", DataSourceHelper.DEFAULT_SCHEMA);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        assertTrue(EXPECTED_TABLE_NAMES.contains(rs.getString("table_name")));
                    }
                }
            }
        }
    }
}
