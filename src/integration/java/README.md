# Integration Tests
This project provides a set of unit tests to verify the integrity of the database post-migration and to prevent unintended
changes to the database structure.

Integration tests are **REQUIRED** for all tables. The build will fail if a table is missing integration tests.

## Directory Structure
The integration test directory has the following structure. Please place tests in the appropriate directory:

* [schema](../java/com/github/gregwhitaker/dbmigrator/schema) - Integration tests that apply to all tables in the entire schema.
* [table](../java/com/github/gregwhitaker/dbmigrator/table) - Integration tests that apply to a single table within the schema.

## Adding a New Table Integration Test
Follow the steps below to add a new table integration test to the project.

1. Ensure the migration script has been created and the local database instance has been migrated to the new version.

2. Run `generateTableIntegTest` Gradle command to generate the skeleton of the new integration test. The task has the following arguments:

    * `--project-dir` - Directory of the project.
    * `--table-name` - Name of the database table for which to generate an integration test.
    * `--force` - Optional argument to overwrite existing integration test. Defaults to `false`.
    
   Example:
   ```
   ./gradlew generateTableIntegTest --args="--table-name=cp_global_product --project-dir=/Users/greg/workspace/dbmigrator"
   ```
   
3. Add the newly created integration test to the [IntegrationTestSuite](../java/com/github/gregwhitaker/dbmigrator/IntegrationTestSuite.java) `SuiteClasses`
annotation.

## Running Integration Tests
Run the following command to execute the integration tests:

    ./gradlew integration
