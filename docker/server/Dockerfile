FROM openjdk:8-jdk AS base

RUN git clone https://github.com/FRINXio/conductor.git

# Conductor project version. Needed to pull additional .jar files.
# One of the following commands run in conductor repo folder returns the version number:
# git describe | grep -oP "\d+\.\d+\.\d+"
# git for-each-ref refs/tags --sort=-taggerdate --format='%(tag)' --count=1 | cut -d "v" -f 2

# Create gradle properties file
RUN cd conductor && echo 'git.root=../../' > gradle.properties
RUN cd conductor && echo "submodVersion=$(git for-each-ref refs/tags --sort=-taggerdate --format='%(tag)' --count=1 | cut -d "v" -f 2)" >> gradle.properties

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
