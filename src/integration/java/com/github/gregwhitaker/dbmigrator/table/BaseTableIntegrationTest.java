package com.github.gregwhitaker.dbmigrator.table;

import com.github.gregwhitaker.dbmigrator.util.DataSourceHelper;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base class that all test classes that deal with a specific database table must extend.
 */
public abstract class BaseTableIntegrationTest {

    protected final String tableName;
    private final Map<String, ExpectedColumnInformation> expectedColumnInfo;
    private List<String> primaryKeys;
    private List<String> columnNames;
    private Map<String, ColumnInformation> columnInfo;
    private List<ForeignKeyRelationship> foreignKeyRelationships;
    private List<IndexInformation> indexes;

    public BaseTableIntegrationTest(String tableName, Map<String, ExpectedColumnInformation> expectedColumnInfo) {
        this.tableName = tableName;
        this.expectedColumnInfo = expectedColumnInfo;
    }

    //
    // Base Tests
    //

    /**
     * Verifies that the table has the correct number of columns and that they are correctly named.
     *
     * @throws SQLException
     */
    @Test
    public void shouldHaveCorrectNumberAndNameOfColumns() throws SQLException {
        final List<String> columnNames = getColumnNames();

        assertEquals(expectedColumnInfo.size(), columnNames.size());
        assertTrue(columnNames.containsAll(expectedColumnInfo.keySet()));
    }

    /**
     * Verifies that the table has the correct column data types.
     *
     * @throws SQLException
     */
    @Test
    public void shouldHaveCorrectColumnDataTypes() throws SQLException {
        getColumnInfo().forEach((s, columnInformation) -> {
            final ExpectedColumnInformation expectedColumnInformation = expectedColumnInfo.get(s);
            assertEquals(String.format("Incorrect column type for: %s", s), expectedColumnInformation.getColumnType(), columnInformation.getColumnType());
        });
    }

    /**
     * Verifies that the table has the correct nullable columns.
     *
     * @throws SQLException
     */
    @Test
    public void shouldHaveCorrectNullableColumns() throws SQLException {
        getColumnInfo().forEach((s, columnInformation) -> {
            final ExpectedColumnInformation expectedColumnInformation = expectedColumnInfo.get(s);
            assertEquals(String.format("Incorrect column nullable: %s", s), expectedColumnInformation.getIsNullable(), columnInformation.getIsNullable());
        });
    }

    /**
     * Verifies that each column in the table has the correct default value.
     *
     * @throws SQLException
     */
    @Test
    public void shouldHaveCorrectColumnDefaults() throws SQLException {
        getColumnInfo().forEach((s, columnInformation) -> {
            final ExpectedColumnInformation expectedColumnInformation = expectedColumnInfo.get(s);
            assertEquals(String.format("Incorrect column default value: %s", s), expectedColumnInformation.getDefaultValue(), columnInformation.getColumnDefault());
        });
    }

    //
    // Helpers
    //

    /**
     * Gets a list of the column names of the primary keys for the table.
     *
     * @return list of primary key column names
     * @throws SQLException
     */
    protected List<String> getPrimaryKeys() throws SQLException {
        if (primaryKeys == null) {
            primaryKeys = new ArrayList<>();

            try (Connection conn = DataSourceHelper.getInstance().getDataSource().getConnection()) {
                final String sql = String.format("SELECT key_column_usage.column_name " +
                        "FROM   information_schema.key_column_usage " +
                        "WHERE  table_schema = '%s' " +
                        "AND    constraint_name = 'PRIMARY' " +
                        "AND    table_name = ?", DataSourceHelper.DEFAULT_SCHEMA);

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, tableName);

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            primaryKeys.add(rs.getString("column_name"));
                        }
                    }
                }
            }
        }

        return primaryKeys;
    }

    /**
     * Gets the list of all column names on the table.
     *
     * @return list of column names
     * @throws SQLException
     */
    protected List<String> getColumnNames() throws SQLException {
        if (columnNames == null) {
            columnNames = new ArrayList<>();

            try (Connection conn = DataSourceHelper.getInstance().getDataSource().getConnection()) {
                final String sql = String.format("SELECT column_name " +
                        "FROM   information_schema.columns " +
                        "WHERE  table_schema = '%s' " +
                        "AND    table_name = ?", DataSourceHelper.DEFAULT_SCHEMA);

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, tableName);

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            columnNames.add(rs.getString("column_name"));
                        }
                    }
                }
            }
        }

        return columnNames;
    }

    /**
     * Gets the list of all column information for the table.
     *
     * @return map of column name to column information
     * @throws SQLException
     */
    protected Map<String, ColumnInformation> getColumnInfo() throws SQLException {
        if (columnInfo == null) {
            columnInfo = new HashMap<>();

            try (Connection conn = DataSourceHelper.getInstance().getDataSource().getConnection()) {
                final String sql = String.format("SELECT * " +
                        "FROM   information_schema.columns " +
                        "WHERE  table_schema = '%s' " +
                        "AND    table_name = ?", DataSourceHelper.DEFAULT_SCHEMA);

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, tableName);

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
                                            rs.getString("column_type"),
                                            rs.getString("column_key"),
                                            rs.getString("extra")
                                    )
                            );
                        }
                    }
                }
            }
        }

        return columnInfo;
    }

    /**
     * Gets foreign keys on the current table.
     *
     * @return foreign keys
     * @throws SQLException
     */
    protected List<ForeignKeyRelationship> getForeignKeyRelationships() throws SQLException {
        if (foreignKeyRelationships == null) {
            foreignKeyRelationships = new ArrayList<>();

            try (Connection conn = DataSourceHelper.getInstance().getDataSource().getConnection()) {
                final String sql = String.format("SELECT table_name, column_name, constraint_name, referenced_table_name, referenced_column_name " +
                        "FROM   information_schema.key_column_usage " +
                        "WHERE  referenced_table_schema = '%s' " +
                        "AND    table_name = ?", DataSourceHelper.DEFAULT_SCHEMA);

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, tableName);

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            foreignKeyRelationships.add(new ForeignKeyRelationship(
                                    rs.getString("table_name"),
                                    rs.getString("column_name"),
                                    rs.getString("constraint_name"),
                                    rs.getString("referenced_table_name"),
                                    rs.getString("referenced_column_name")
                            ));
                        }
                    }
                }
            }
        }

        return foreignKeyRelationships;
    }

    /**
     * Gets index on the current table (excluding primary key auto-generated indexes).
     *
     * @return indexes
     * @throws SQLException
     */
    protected List<IndexInformation> getIndexes() throws SQLException {
        if (indexes == null) {
            indexes = new ArrayList<>();

            try (Connection conn = DataSourceHelper.getInstance().getDataSource().getConnection()) {
                final String sql = String.format("SELECT * " +
                        "FROM   information_schema.statistics " +
                        "WHERE  table_schema = '%s' " +
                        "AND    table_name = ? " +
                        "AND    index_name != 'PRIMARY'", DataSourceHelper.DEFAULT_SCHEMA);

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, tableName);

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            indexes.add(new IndexInformation(
                                    rs.getString("table_schema"),
                                    rs.getString("table_name"),
                                    rs.getInt("non_unique"),
                                    rs.getString("index_schema"),
                                    rs.getString("index_name"),
                                    rs.getString("column_name")
                            ));
                        }
                    }
                }
            }
        }

        return indexes;
    }

    //
    // Domain
    //

    /**
     * Table column information.
     */
    @Data
    @AllArgsConstructor
    public static class ColumnInformation {
        private String tableSchema;
        private String tableName;
        private String columnName;
        private Integer ordinalPosition;
        private String columnDefault;
        private Boolean isNullable;
        private String dataType;
        private String columnType;
        private String columnKey;
        private String extra;
    }

    /**
     * Expected table column information for testing.
     */
    @Data
    @AllArgsConstructor
    public static class ExpectedColumnInformation {
        private String columnType;
        private Boolean isNullable;
        private String defaultValue;

        public ExpectedColumnInformation(final String columnType, final Boolean isNullable) {
            this.columnType = columnType;
            this.isNullable = isNullable;
        }
    }

    /**
     * Table foreign key constraint relationship
     */
    @Data
    @AllArgsConstructor
    public static class ForeignKeyRelationship {
        private String tableName;
        private String columnName;
        private String constraintName;
        private String referencedTableName;
        private String referencedColumnName;
    }

    /**
     * Table index information.
     */
    @Data
    @AllArgsConstructor
    public static class IndexInformation {
        private String tableSchema;
        private String tableName;
        private Integer nonUnique;
        private String indexSchema;
        private String indexName;
        private String columnName;
    }
}
