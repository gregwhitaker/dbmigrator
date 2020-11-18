# migration
Database migration scripts for the database.

If you have migration scripts that should only execute in certain environments, please place them in the appropriate
environment folder within the [migration-env](../migration-env) directory.

## Creating a New Migration
Run the following command to create a new Java-based migration in this directory:

    ./gradlew createMigrationScript --type="{migration type}" --ver="{version}" --desc="{description}"

Example:

    ./gradlew createMigrationScript --type="V" --ver="1.0.21" --desc="Add Table Foo"
    
The newly created script can be found in the script migration directory (`src/main/resources/db/migration`).
