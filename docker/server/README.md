# Docker
## Conductor server
This Dockerfile create the conductor:server image

## Building the image

Run the following commands from the project root.

`docker build -f docker/server/Dockerfile -t conductor:server .`

## Running the conductor server
Conductor server requires redis, postgres and elastic search.
In order to build using Redis and Elastic Search `docker-compose -f docker-compose.yaml build` and to run the docker image `docker-compose -f docker-compose.yaml up`
Postgres and Elastic Search check [community](community-link)
