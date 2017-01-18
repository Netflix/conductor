# Netflix Conductor (Sample) Docker Image
A docker image that builds the Netflix Conductor on Ubuntu w

This image runs the sample conductor server described in the [Getting Started Guide](https://netflix.github.io/conductor/intro/). If the server dies/stops data will not be persisted

The built image can be found at [Docker Hub](https://hub.docker.com/r/jcantosz/netflix-conductor-sample/)

# Running the docker image
Use the following command:
`docker run -t -i -p 3000:3000 -p 8080:8080 jcantosz/netflix-conductor-sample`
(wait for gradle & npm installs)

NOTE: The gradle build process will not exit (as it is running the server)

Browse to [http://localhost:3000](http://localhost:3000) for the UI and [http://localhost:8080/swagger-ui](http://localhost:8080/swagger-ui) for the swagger docs

## Building from Source
Clone this repo:
`git clone https://github.com/Netflix/conductor`

Change to the docker directory
`cd docker`

Build the image:
`docker build -t netflix-conductor-sample .`
