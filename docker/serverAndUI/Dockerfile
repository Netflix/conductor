#
# conductor:serverAndUI - Netflix conductor server and UI
#
FROM openjdk:8-jdk

MAINTAINER Netflix OSS <conductor@netflix.com>
RUN apt-get update -y && apt-get -y install git curl
RUN curl -sL https://deb.nodesource.com/setup_10.x |  bash
RUN apt-get install -y nodejs
RUN apt-get install -y npm

# Make app folders
RUN mkdir -p /app/config /app/logs /app/libs /app/ui
COPY ./ui /app/ui
COPY docker/serverAndUI/bin /app
COPY docker/serverAndUI/config /app/config
COPY server/build/libs/conductor-server-*-all.jar /app/libs
COPY docker/server/newrelic-agent-5.4.0 /newrelic


RUN cd /app/ui \
  && npm install -s \
  && npm run build -s --server

EXPOSE 5000 8080

RUN chmod +x /app/startup.sh
CMD ["/app/startup.sh"]
ENTRYPOINT ["/bin/bash"]