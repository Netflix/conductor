FROM java:8-jre-alpine

RUN apk update && apk add bash

# Make app folders
RUN mkdir -p /app

# Copy the project directly onto the image
COPY ./docker/archiver/archiver.properties /app
COPY ./archiver/build/distributions/conductor-archiver.zip /

RUN unzip conductor-archiver.zip
RUN rm conductor-archiver.zip
RUN mv conductor-archiver/* /app
RUN rm -rf conductor-archiver

# Copy the files for the server into the app folders
RUN chmod +x /app/bin/conductor-archiver

WORKDIR /app

CMD [ "/app/bin/conductor-archiver" ]