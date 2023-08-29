# Docker
## Conductor server
This Dockerfile create the conductor:server image

## Building the image

In order to build the conductor:server image run the following command from the project root,

`docker build -f docker/server/Dockerfile -t conductor:server .`

The above image requires redis and elastic search to be running externally. 

In order to build the standalone image which contains server, ui, redis and elasticsearch run the following commands from the project root.

`docker-compose -f docker/docker-compose.yaml build`

## Running the conductor server
Run the following commands from the project root.
`docker-compose -f docker-compose.yaml up`

For Postgres and Elastic Search 7 setup check [community](https://github.com/Netflix/conductor-community/tree/main/docker)
