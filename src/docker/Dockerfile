FROM adoptopenjdk/openjdk11
ARG APP_NAME
VOLUME /tmp
RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        unzip\
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/dbmigrator
COPY build/distributions/${APP_NAME}.zip /opt/dbmigrator/app.zip
RUN unzip /opt/dbmigrator/app.zip -d /opt/dbmigrator \
    && mv /opt/dbmigrator/${APP_NAME} /opt/dbmigrator/app
ENTRYPOINT ["/opt/dbmigrator/app/bin/dbmigrator"]
