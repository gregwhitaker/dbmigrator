package com.github.gregwhitaker.dbmigrator;

import com.github.gregwhitaker.dbmigrator.schema.IntegrationSanityCheckTest;
import com.github.gregwhitaker.dbmigrator.schema.SchemaIntegrationTest;
import com.github.gregwhitaker.dbmigrator.util.DataSourceHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test suite.
 *
 * Add new test classes to the {@link org.junit.runners.Suite.SuiteClasses} annotation below to have them included
 * in the test run.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        // Add new table integration test classes here

        // Add new schema integration test classes here
        IntegrationSanityCheckTest.class,
        SchemaIntegrationTest.class
})
public class IntegrationTestSuite {
    private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestSuite.class);

    @BeforeClass
    public static void setupSuite() {
        LOG.info("Running database migrator for IntegrationTestSuite");

        DatabaseMigrator migrator = new DatabaseMigrator(DataSourceHelper.getInstance().getDataSource());
        migrator.run("integration", true);
    }

    @AfterClass
    public static void teardownSuite() {
        // Noop
    }
}
