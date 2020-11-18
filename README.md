# dbmigrator
Utility for managing database migration scripts and versioning.

## Project Structure
Migrations are stored in the following directories (please refer to the links below for more information):

* [Flyway Script Migrations](/src/main/resources/db) - Migration scripts both universal and environment-specific.
* [Flyway Java Migrations](/src/main/java/db/migration) - Java-based migrations.

## Development
Common tasks for developing and testing migrations locally are detailed below:

### Adding New Script Migration
Run the following command to create a new script migration:

    ./gradlew createMigrationScript --type="{migration type}" --ver="{version}" --desc="{description}"
    
Example:

    ./gradlew createMigrationScript --type="V" --ver="1.0.21" --desc="Add Table Foo"
    
The newly created script can be found in the script migration directory (`src/main/resources/db/migration`).

If you wish to create an environment-specific migration script, simply include the optional `--env` parameter:

    ./gradlew createMigrationScript --type="{migration type}" --ver="{version}" --desc="{description}" --env="{environment}"
    
Example:

    ./gradlew createMigrationScript --type="V" --ver="1.0.21" --desc="Add Table Foo" --env="test"

The newly created script can be found in the test script migration directory (`src/main/resources/db/migration-env/test`).

### Adding New Java Migration
Run the following command to create a new Java migration:

    ./gradlew createMigrationClass --type="{migration type}" --ver="{version}" --desc="{description}"
    
Example:

    ./gradlew createMigrationClass --type="V" --ver="1.0.21" --desc="Add Table Foo"
    
The newly created class can be found in the java migration package (`src/main/java/db/migration`).

### Building
Run the following command to build the application:

    ./gradlew clean build

Run the following command to build the application as a Docker image (Executable Jar):

    ./gradlew clean buildImage
    
Run the following command to build the application as a Docker image (Spring Boot):

    ./gradlew clean buildBootImage

### Running Migrations Locally
The project is configured to use Docker Compose for testing migrations locally and there are a number of Gradle tasks
provided to make the task easy.

#### Migrate Database
Run the following command to execute the database migration:

    ./gradlew migrateDb

#### Delete Database    
Run the following command to delete the database:

    ./gradlew cleanDb
    
#### Delete Database and Run Migration
Run the following command to delete the database and rerun the migration:

    ./gradlew cleanMigrateDb
    
#### Delete Database and Mounted Data Volumes
The MySQL container makes use of volumes to store data across container invocations. Run the following command to delete
the database, the associated data volumes, and start with a fresh instance:

    ./gradlew killDb

## Integration Testing
The project contains a set of integration tests for validating the database schema post migration. Run the following command
to execute the integration tasks locally:

    ./gradlew clean build integrationTest
    
For more information on adding integration tests, please refer to the [Integration Test Documentation](/src/integration/README.md).

