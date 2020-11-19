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
            assertEquals(String.format("Incorrect column type for: %s", s), expectedColumnInformation.getColumnType(), columnInformation.getUdtName());
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
                final String sql =
                        "SELECT     c.column_name, c.ordinal_position " +
                        "FROM       information_schema.key_column_usage AS c " +
                        "LEFT JOIN  information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name " +
                        "WHERE      t.table_schema = ? " +
                        "AND        t.table_name = ? " +
                        "AND        t.constraint_type = 'PRIMARY KEY' " +
                        "ORDER BY   c.ordinal_position";

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, DataSourceHelper.DEFAULT_SCHEMA);
                    ps.setString(2, tableName);

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
                final String sql =
                        "SELECT column_name " +
                        "FROM   information_schema.columns " +
                        "WHERE  table_schema = ? " +
                        "AND    table_name = ?";

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, DataSourceHelper.DEFAULT_SCHEMA);
                    ps.setString(2, tableName);

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
                final String sql =
                        "SELECT * " +
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
                final String sql =
                    "SELECT " +
                            "att2.attname AS column_name, " +
                            "cl.relname AS referenced_table_name, " +
                            "att.attname AS referenced_column_name, " +
                            "conname AS constraint_name " +
                    "FROM " +
                            "(SELECT " +
                                    "unnest(con1.conkey) AS parent, " +
                                    "unnest(con1.confkey) AS child, " +
                                    "con1.confrelid, " +
                                    "con1.conrelid, " +
                                    "con1.conname " +
                                    "FROM pg_class cl " +
                                    "JOIN pg_namespace ns ON cl.relnamespace = ns.oid " +
                                    "JOIN pg_constraint con1 ON con1.conrelid = cl.oid " +
                                    "WHERE cl.relname = ? " +
                                    "AND ns.nspname = ? " +
                                    "AND con1.contype = 'f' " +
                            ") con " +
                    "JOIN pg_attribute att ON att.attrelid = con.confrelid AND att.attnum = con.child " +
                    "JOIN pg_class cl ON cl.oid = con.confrelid " +
                    "JOIN pg_attribute att2 ON att2.attrelid = con.conrelid AND att2.attnum = con.parent";

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, tableName);
                    ps.setString(2, DataSourceHelper.DEFAULT_SCHEMA);

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            foreignKeyRelationships.add(new ForeignKeyRelationship(
                                    tableName,
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
     * Gets indexes on the current table.
     *
     * @return list of {@link IndexInformation}
     * @throws SQLException
     */
    protected List<IndexInformation> getIndexes() throws SQLException {
        if (indexes == null) {
            indexes = new ArrayList<>();

            try (Connection conn = DataSourceHelper.getInstance().getDataSource().getConnection()) {
                final String sql =
                        "SELECT " +
                            "ns.nspname AS table_schema, " +
                            "idx.indrelid :: REGCLASS AS table_name, " +
                            "i.relname AS index_name, " +
                            "idx.indisunique AS is_unique, " +
                            "idx.indisprimary AS is_primary, " +
                            "ARRAY( " +
                                    "SELECT pg_get_indexdef(idx.indexrelid, k + 1, TRUE) " +
                                    "FROM generate_subscripts(idx.indkey, 1) AS k " +
                                    "ORDER BY k " +
                            ") AS column_names, " +
                            "(idx.indexprs IS NOT NULL) OR (idx.indkey::int[] @> array[0]) AS is_functional, " +
                            "idx.indpred IS NOT NULL AS is_partial " +
                        "FROM pg_index AS idx " +
                        "JOIN pg_class AS i ON i.oid = idx.indexrelid " +
                        "JOIN pg_am AS am ON i.relam = am.oid " +
                        "JOIN pg_namespace AS ns ON i.relnamespace = ns.OID " +
                        "JOIN pg_user AS U ON i.relowner = u.usesysid " +
                        "WHERE NOT nspname LIKE ? " +
                        "AND idx.indrelid = (SELECT ?::regclass::oid) " +
                        "AND idx.indisprimary = false";

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, DataSourceHelper.DEFAULT_SCHEMA);
                    ps.setString(2, tableName);

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            indexes.add(new IndexInformation(
                                    rs.getString("table_schema"),
                                    rs.getString("table_name"),
                                    rs.getString("index_name"),
                                    rs.getBoolean("is_unique"),
                                    rs.getBoolean("is_primary"),
                                    (String[]) rs.getArray("column_names").getArray(),
                                    rs.getBoolean("is_functional"),
                                    rs.getBoolean("is_partial")
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
        private Long characterMaximumLength;
        private String udtName;
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
        private String indexName;
        private Boolean isUnique;
        private Boolean isPrimary;
        private String[] columnNames;
        private Boolean isFunctional;
        private Boolean isPartial;
    }
}
