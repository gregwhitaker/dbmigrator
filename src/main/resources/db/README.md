# Database Migration
Database migration scripts for the database.

Migration scripts are managed via [Flyway](https://www.flywaydb.org).

## Directory Structure
The migrations directory is structured as follows:

* [migration](migration) - Migration scripts for the database.
* [migration-env](migration-env) - Environment-specific migration scripts for the database.
* [support](support) - Adhoc scripts for support and ongoing maintenance of the database.

## Naming Convention
All database migration scripts must follow the naming conventions below.

### Versioned Scripts
All versioned database migration scripts must be named according to the following pattern:

    V{version}_{timestamp}__{description}.sql

Example: `V0_1_0_202001061041__create_schema.sql`
    
**Version:** Semantic version of the platform with `.` replaced by `_`.

- `1.0.12` would become `1_0_12`.

**Timestamp:** Date and time of file creation in the format: `yyyyMMddhhmmss`.

**Description:** A short description of the migration with spaces replaced by `_`.

### Repeatable Scripts
All repeatable migration scripts must be named according to the following pattern:

    R__{description}.sql

Example: `R__load_test_data.sql`

**Description:** A short description of the migration with spaces replaced by `_`.