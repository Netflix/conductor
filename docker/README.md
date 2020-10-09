# Docker

This dockerfile runs using dynomite and elasticsearch. Conductor is split into backend (server) and frontend (ui) services. The server service is responsible for providing a RESTful interface for all workflow configuration and processing.  The ui service implements a user interface to ease workflow management.

## Building Conductor

The build process for Java requires 3 docker commands. The first command installs the server source code onto to the Java JDK image.  The second command executes the build process and writes the JAR files back to the project folder at server/build.  Finally, the third command takes the built JAR files and installs them onto the Java 8 JRE image.  The end result should be a runtime image which is roughly 150 MB in size.

**server:**

`docker build --no-cache -t conductor:server-build -f docker/server/Dockerfile.build .`

`docker run --rm -v $(pwd)/server/build:/conductor/server/build conductor:server-build`

`docker build --no-cache -t conductor:server -f docker/server/Dockerfile .`

**ui:**

`docker build --no-cache -t conductor:ui -f docker/ui/Dockerfile .`

## Running Conductor

Use docker-compose to start and stop Conductor.

 `docker-compose -f docker/docker-compose.yml up -d`
 
 `docker-compose -f docker/docker-compose.yml down`

Environment Variables:

 - **db** (memory | redis | dynomite) - whether to run the data store and search index in memory or externally
 - **workflow_dynomite_cluster_hosts** - hostname, port and region for the external dynomite data store
 - **workflow_dynomite_cluster_name** - name of the dynomite cluster
 - **workflow_namespace_prefix** - prefix for workflows
 - **workflow_namespace_queue_prefix** - prefix for queues
 - **queues_dynomite_threads** - number of dynomite queue threads
 - **queues_dynomite_nonQuorum_port** - port for dynomite queue
 - **workflow_elasticsearch_url** - host and port to use for elasticsearch index
 - **workflow_elasticsearch_index_name** - name for elasticsearch index
 - **loadSample** - whether or not to create kitchensink workflow on initialization

## Inspecting Running Conductor Containers

The following commands will run the container without running the startup script.  This will give you and opportunity to inspect the file system and test executing the startup script or other commands directly.

 - `docker run -it conductor:server /bin/sh`
 - `docker run -it conductor:ui /bin/sh`
