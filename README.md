![Conductor](docs/docs/img/conductor-vector-x.png)

## Conductor
Forked from [netflix-conductor](https://github.com/Netflix/conductor)

Conductor is an _orchestration_ engine that runs in the cloud.


[![License](https://img.shields.io/github/license/Netflix/conductor.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![NetflixOSS Lifecycle](https://img.shields.io/osslifecycle/Netflix/conductor.svg)]()

## Community
[![Gitter](https://badges.gitter.im/netflix-conductor/community.svg)](https://gitter.im/netflix-conductor/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge) Please feel free to join our Gitter for questions and interacting with the community.

## Documentation & Getting Started
[Wiki](https://foxsportsau.atlassian.net/wiki/spaces/DEV/pages/442138974/Conductor)

[Getting Started](https://netflix.github.io/conductor/gettingstarted/basicconcepts/) guide.

## Building
To build the server, use the following dependencies in your classpath:
```bash
./gradlew build -x test
```

## Running
Minimal run with in mem database & elasticsearch. Mind the HTTP port you pick:
```bash
env db=memory; 
env workflow.elasticsearch.instanceType=memory; 
env conductor.jetty.server.port=8080; 
java -jar  ./server/build/libs/conductor-server-2019.1.0-SNAPSHOT-all.jar
```
Since we package and publish conductor as container, you can also quick star using docker.
Below start a ready to use conductor server with in memory mode for DB and ElasticSearch:
```bash
docker run  -p 8080:8080 -edb=memory -eworkflow.elasticsearch.instanceType=memory kayosportsau/netflixconductor:latest
```
Then you can access the [swagger API](http://localhost:8080/index.html) and schedule/execute tasks and workflows.

If you need both conductor-api and conductor-ui, create a network and start both containers on it:
```bash
docker network create --driver bridge my-conductor
docker run --name conductor --rm  -p 8080:8080 --network my-conductor -edb=memory -eworkflow.elasticsearch.instanceType=memory kayosportsau/netflixconductor:latest
docker run --network my-conductor -eWF_SERVER='http://conductor:8080/api/' -p 5000:5000 kayosportsau/netflixconductor:ui-v2019.0.60```
```
Above
 
(1) create a bridge network named `my-conductor`

(2) start conductor server in that network `--network my-conductor` with container name `--name conductor`. Other container in the same network can refer to this one via this name as hostname.

(3) start conductor ui on the bridge network, with en var WF_SERVER referring to api container
Now you should be able to access UI via localhost:5000

## License
Copyright 2018 Netflix, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
