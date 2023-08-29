# Docker
## Conductor server
This Dockerfile create the conductor:server image

## Building the image

Run the following commands from the project root.

`docker build -f docker/server/Dockerfile -t conductor:server .`

## Running the conductor server
Conductor server requires redis, postgres and elastic search.
In order to build and run using Redis and Elastic Search 6 run from the docker directory, `docker-compose -f docker-compose.yaml up`
Postgres and Elastic Search 7 check [community](community-link)
