# docker
Docker compose configurations for working the Database Migrator.

* [docker-compose-postgres.yml](docker-compose-postgres.yml) - Compose file that starts the PostgreSQL database.
* [docker-compose-all.yml](docker-compose-all.yml) - Compose file that starts both the PostgreSQL database and the Database Migrator.

## Running with Compose
Run the following command to start the database only:

    docker-compose up
    
Run the following command to start both the database and the Database Migrator:

    docker-compose --file docker-compose-all.yml up
    
## Stopping with Compose
The Postgres Docker container uses volumes to persist data across container invocations. Run the following command to stop
the container and remove the data volumes:

    docker-compose down -v
    
If you are running both the database and the Database Migrator, run the following command to stop the containers and
remove the data volumes:

    docker-compose --file docker-compose-all.yml down -v
    
## Development
There are a number of helpful Gradle tasks to execute the Compose files during development that can be found by running
the following command:

    ./gradlew tasks
