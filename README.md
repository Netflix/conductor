![Conductor](docs/docs/img/conductor-vector-x.png)

## Conductor
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
