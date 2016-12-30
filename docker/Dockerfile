FROM ubuntu
MAINTAINER Jesse Antoszyk <jesse@antoszyk.com>

COPY tmp/startup.sh startup.sh

#Install Packages
RUN apt-get update -y \
    && apt-get install -y software-properties-common python-software-properties apt-transport-https \

    # Install DynomiteDB
    && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys FB3291D9 \
    && add-apt-repository "deb https://apt.dynomitedb.com/ trusty main" \

    # Install NodeJS
    && apt-get install -y curl \
    && curl -sL https://deb.nodesource.com/setup_6.x |  bash - \
    && apt-get install -y git dynomitedb nodejs build-essential default-jre default-jdk \

    # Install Tomcat
    && groupadd tomcat \
    && useradd -s /bin/false -g tomcat -d /opt/tomcat tomcat \
    && cd /tmp \
    && curl -O http://mirrors.ocf.berkeley.edu/apache/tomcat/tomcat-8/v8.5.9/bin/apache-tomcat-8.5.9.tar.gz \
    && mkdir /opt/tomcat \
    && tar -xzvf apache-tomcat-8*tar.gz -C /opt/tomcat --strip-components=1 \
    && rm -rf apache-tomcat-8*tar.gz \
    && cd /opt/tomcat \
    && chgrp -R tomcat /opt/tomcat \
    && chmod -R g+r conf \
    && chmod g+x conf \
    && chown -R tomcat webapps/ work/ temp/ logs/ \

    # Install Netflix Conductor
    && mkdir /netflix \
    && cd /netflix \
    && git clone https://github.com/Netflix/conductor.git

# Expose Ports
EXPOSE 3000 8080

# Install and run server
ENTRYPOINT ["/startup.sh"]

