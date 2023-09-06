# Conductor Docker Builds

## Pre-built docker images

Conductor server with support for the following backend:
1. Redis
2. Postgres
3. Mysql
4. Cassandra

```shell
docker pull docker.orkes.io/conductor:latest
```

### Docker File for Server and UI

[Docker Image Source for Server with UI](serverAndUI/Dockerfile)

#### Pre-requisites for building
1. [Docker](https://www.docker.com/)
2. [Node](https://nodejs.org/en)
3. [JDK](https://openjdk.org/)

### Configuration Guide for Conductor Server
Conductor uses a persistent store for managing state.  
The choice of backend is quite flexible and can be configured at runtime using `conductor.db.type` property.

Refer to the table below for various supported backend and required configurations to enable each of them.

| Backend    | Property                           | Required Configuration |
|------------|------------------------------------|------------------------|
| postgres   | conductor.db.type=postgres         |                        |
| redis      | conductor.db.type=redis_standalone |                        |
| mysql      | conductor.db.type=mysql            |                        |
| cassandra  | conductor.db.type=cassandra        |                        |    

Conductor using Elasticsearch for indexing the workflow data.  
Currently, Elasticsearch 6 and 7 are supported.
We welcome community contributions for other indexing backends.

**Note:** Docker images use Elasticsearch 7.

### Recommended Configuration for the server
```properties

```

## Helm Charts
TODO: Link to the helm charts

## Run Docker Compose Locally
