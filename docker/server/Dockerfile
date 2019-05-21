FROM openjdk:8-jdk AS base

COPY ./ /conductor

# Download and build .jar files needed
RUN cd conductor/server && ../gradlew build --no-daemon -x test

#
# conductor:server - Netflix conductor server
#
FROM java:8-jre-alpine

# Make app folders
RUN mkdir -p /app/config /app/logs /app/libs

# Copy the project directly onto the image
COPY --from=base /conductor/docker/server/bin /app
COPY --from=base /conductor/docker/server/config /app/config
COPY --from=base /conductor/server/build/libs/conductor-server-*-all.jar /app/libs

# Copy the files for the server into the app folders
RUN chmod +x /app/startup.sh

EXPOSE 8080

CMD [ "/app/startup.sh" ]
ENTRYPOINT [ "/bin/sh"]
