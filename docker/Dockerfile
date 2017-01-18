FROM ubuntu
MAINTAINER Jesse Antoszyk <jesse@antoszyk.com>

COPY tmp/startup.sh startup.sh

#Install Packages
RUN apt-get update -y \

    # Install NodeJS
    && apt-get install -y curl \
    && curl -sL https://deb.nodesource.com/setup_6.x |  bash - \
    && apt-get install -y git nodejs build-essential default-jre default-jdk \

    # Install Netflix Conductor
    && mkdir /netflix \
    && cd /netflix \
    && git clone https://github.com/Netflix/conductor.git

# Expose Ports
EXPOSE 3000 8080

# Install and run server
ENTRYPOINT ["/startup.sh"]

