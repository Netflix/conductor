FROM java:8-jdk as builder
WORKDIR /stage
ADD . /stage

RUN ./gradlew build -x test

#
# conductor:archiver - Deluxe conductor archiver
#
FROM java:8-jre

# Make app folders
RUN mkdir -p /app

# Copy the project directly onto the image
COPY --from=builder /stage/docker/archiver/bin/startup.sh /app
COPY --from=builder /stage/archiver/build/libs/conductor-archiver-*-all.jar /app

RUN chmod +x /app/startup.sh

CMD [ "/app/startup.sh" ]