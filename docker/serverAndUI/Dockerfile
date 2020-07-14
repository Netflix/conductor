#
# conductor:serverAndUI - Netflix conductor server and UI
# This is a local friendly distribution.
# BUILD:
#    docker build ./docker/serverAndUI -f docker/serverAndUI/Dockerfile -t conductor:all-latest
# RUN:
#    docker run --rm --name conductor --rm -p 9000:8080 -p 5000:5000  -edb=memory -eworkflow.elasticsearch.instanceType=memory conductor:all-latest
FROM amazoncorretto:8

MAINTAINER Streamotion Pty Ltd <devops@kayosports.com.au>

RUN yum update -y \
  && yum install -y  telnet \
  && yum install git -y \
  && yum install -y gcc-c++ make \
  && yum clean all

# Make app folders
RUN mkdir -p /app/config /app/logs /app/libs

# Startup script(s)
COPY docker/serverAndUI/bin /app

# Configs
COPY docker/serverAndUI/config /app/config

# Get all the dependencies
RUN chmod +x /app/startup.sh \
  && curl -sL https://rpm.nodesource.com/setup_10.x | bash - \
  && yum install -y nodejs

# Get and install conductor
RUN git clone https://github.com/Netflix/conductor.git \
  && cd conductor \
  && ./gradlew build -x test \
  && mv ./server/build/libs/conductor-server-*-all.jar /app/libs/ \
  && mv ./ui /app \
  && cd /app/ui \
  && npm install \
  && npm run build --server \
  && cd / \
  && rm -rf conductor

EXPOSE 5000 8080

RUN chmod +x /app/startup.sh
CMD ["/app/startup.sh"]
ENTRYPOINT ["/bin/bash"]