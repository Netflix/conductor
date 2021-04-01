FROM java:8-jdk as builder
WORKDIR /stage
ADD . /stage

ARG SONAR_ENABLED=0

RUN ./gradlew build -x test
RUN if [ "${SONAR_ENABLED}" -eq "1" ]; then ./gradlew sonarqube -x test -Dsonar.verbose=true -Dsonar.projectKey=conductor -Dsonar.branch.name="${SONAR_BRANCH}" -Dsonar.host.url="${SONAR_HOST}" -Dsonar.login="${SONAR_LOGIN}"; else echo "SONAR_ENABLED ${SONAR_ENABLED}. Analysis skipped"; fi

#
# conductor:server - Netflix conductor server
#
FROM java:8-jre

MAINTAINER Netflix OSS <conductor@netflix.com>

# Make app folders
RUN mkdir -p /app/config /app/logs /app/libs

# Copy the project directly onto the image
COPY --from=builder /stage/docker/server/bin /app
COPY --from=builder /stage/docker/server/config /app/config
COPY --from=builder /stage/docker/server/lib/dd-java-agent-0.46.0.jar /app/libs
COPY --from=builder /stage/server/build/libs/conductor-server-*-all.jar /app/libs

# Copy the files for the server into the app folders
RUN chmod +x /app/startup.sh

EXPOSE 8080
CMD [ "/app/startup.sh" ]