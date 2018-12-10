FROM java:8-jre-alpine

RUN apk update && apk add bash

# Make app folders
RUN mkdir -p /app

# Copy the project directly onto the image
COPY ./docker/archiver/bin/startup.sh /app
COPY ./docker/archiver/config/archiver.properties /app
COPY ./archiver/build/distributions/conductor-archiver.zip /

RUN unzip conductor-archiver.zip
RUN rm conductor-archiver.zip
RUN mv conductor-archiver/* /app
RUN rm -rf conductor-archiver

RUN chmod +x /app/bin/conductor-archiver
RUN chmod +x /app/startup.sh

CMD [ "/app/startup.sh" ]