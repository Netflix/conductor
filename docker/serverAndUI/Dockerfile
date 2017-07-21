#
# conductor:serverAndUI - Netflix conductor server and UI
#
FROM java:8-jdk

MAINTAINER Netflix OSS <conductor@netflix.com>

# Make app folders
RUN mkdir -p /app/config /app/logs /app/libs

# Copy the project directly onto the image
COPY . /conductor

# Copy the files for the server into the app folders
RUN cp -R /conductor/docker/serverAndUI/bin/. /app \
  && cp -R /conductor/docker/serverAndUI/config/. /app/config \
  && chmod +x /app/startup.sh

# Get all the dependencies
RUN apt-get update -y \
  && apt-get -y install curl \

  # Get node
  && curl -sL https://deb.nodesource.com/setup_6.x |  bash - \
  && apt-get install -y nodejs build-essential

# Get and install conductor
RUN /conductor/gradlew build -x test \

  # Get Server Jar
  && mv /conductor/server/build/libs/conductor-server-*-all.jar /app/libs/ \

  # Get UI project
  && mv /conductor/ui /app \

  # Install UI packages
  && cd /app/ui \
  && npm install \
  && npm run build --server \

  # Go back to root
  && cd / \

  # Clean up
  && rm -rf conductor


EXPOSE 5000 8080

CMD ["/app/startup.sh"]
ENTRYPOINT ["/bin/bash"]
