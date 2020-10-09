FROM java:8-jre

#RUN apk update && apk add bash

# Make app folders
RUN mkdir -p /app

# Copy the project directly onto the image
COPY ./docker/archiver/bin/startup.sh /app
COPY ./archiver/build/libs/conductor-archiver-*-all.jar /app

RUN chmod +x /app/startup.sh

CMD [ "/app/startup.sh" ]