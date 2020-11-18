# migration-env
Environment-specific database migration scripts.

Place your migration script in the folder with the name corresponding to the deployment environment where you intend
the script to be ran. The database migration process will automatically detect and run the script when it is executed
within that environment.

## Creating a New Environment-Specific Migration
Run the following command to create a new environment-specific script migration:

    ./gradlew createMigrationScript --type="{migration type}" --ver="{version}" --desc="{description}" --env="{environment}"
    
Example:

    ./gradlew createMigrationScript --type="V" --ver="1.0.21" --desc="Add Table Foo" --env="test"

The newly created script can be found in the test script migration directory (`src/main/resources/db/migration-env/test`).
