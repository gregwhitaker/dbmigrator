package com.github.gregwhitaker.dbmigrator.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

/**
 * Helper class for retrieving a {@link DataSource} configured for the integration test database.
 */
public final class DataSourceHelper {
    private static final DataSourceHelper INSTANCE = new DataSourceHelper();

    public static final String DEFAULT_SCHEMA = "public";
    public static final String DEFAULT_JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    public static final String DEFAULT_USERNAME = "postgres";
    public static final String DEFAULT_PASSWORD = "changeme";

    private final DataSource dataSource;

    private DataSourceHelper() {
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(DEFAULT_JDBC_URL);
        hikariConfig.setUsername(DEFAULT_USERNAME);
        hikariConfig.setPassword(DEFAULT_PASSWORD);
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setInitializationFailTimeout(30_000);

        this.dataSource = new HikariDataSource(hikariConfig);
    }

    /**
     * Gets the singleton instance of {@link DataSourceHelper}.
     *
     * @return datasource helper
     */
    public static DataSourceHelper getInstance() {
        return INSTANCE;
    }

    /**
     * Gets an instance of the local {@link DataSource}.
     *
     * @return datasource
     */
    public DataSource getDataSource() {
        return dataSource;
    }
}
