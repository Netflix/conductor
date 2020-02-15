FROM amazoncorretto:8

MAINTAINER Streamotion Pty Ltd <devops@kayosports.com.au>

RUN yum update -y \
  && yum install -y \
  telnet \
  && yum clean all

# Make app folders
RUN mkdir -p /app/config /app/logs /app/libs

# Copy the project directly onto the image
COPY docker/server/bin /app
COPY docker/server/config /app/config
COPY server/build/libs/conductor-server-*-all.jar /app/libs


COPY docker/server/newrelic-agent-5.4.0 /newrelic
# Copy the files for the server into the app folders
RUN chmod +x /app/startup.sh

EXPOSE 8080
EXPOSE 8090

CMD [ "/app/startup.sh" ]
ENTRYPOINT [ "/bin/sh"]
