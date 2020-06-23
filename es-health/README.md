# Elastic Search Health Check 

This module is optional and can compile with `es5-persistence` or `es6-persistence`.

**!! Only tested with es5**

## Build

### Gradle Configuration

Modify the following files

[settings.gradle](https://github.com/Netflix/conductor/blob/master/settings.gradle)

```diff
@@ -3,6 +3,7 @@ rootProject.name='conductor'
 include 'client','common','contribs','core', 'es5-persistence','jersey', 'postgres-persistence', 'zookeeper-lock', 'redis-lock'
 include 'cassandra-persistence', 'mysql-persistence', 'redis-persistence','server','test-harness','ui'
 include 'grpc', 'grpc-server', 'grpc-client'
+include 'es-health'

 rootProject.children.each {it.name="conductor-${it.name}"}

```

[server/build.gradle](https://github.com/Netflix/conductor/blob/master/server/build.gradle)

```diff
@@ -38,6 +38,7 @@ dependencies {

+   compile project(':conductor-es-health')

    compile "com.netflix.runtime:health-guice:${revHealth}"
```
 
Delete all dependencies.lock files of all modules and then run at the root folder of the project: 

```
./gradlew generateLock saveLock -PdependencyLock.includeTransitives=true
```

Run Build along with the tests

```
./gradlew conductor:es-health build
```

## Configuration

In the `.properties` file of conductor `server` you must add the following configurations.

* Add the ElasticSearchHealthModule module. If you have several modules, separate them with a comma.
```
conductor.additional.modules=com.netflix.conductor.health.ElasticSearchHealthModule
```

* Health Indicator can be configured through HealthAggregatorConfiguration. The following attributes can be added to the properties file
```
health.aggregator.cacheHealthIndicators=true
health.aggregator.cacheIntervalInMillis=5000
health.aggregator.aggregatorWaitIntervalInMillis=1000
```

### Usage

Request: HTTP GET /api/health


* If ElasticSearch is reachable

Response: HTTP 200
```json
{
  "healthResults": [
    {
      "details": {
        "cached": true,
        "className": "com.netflix.conductor.health.ElasticSearchHealthIndicator"
      },
      "healthy": true,
      "errorMessage": {
        "present": false
      }
    }
  ],
  "suppressedHealthResults": [],
  "healthy": true
}
```

* If ElasticSearch is unreachable

Response: HTTP 200
```json
{
  "healthResults": [
    {
      "details": {
        "error": "org.elasticsearch.client.transport.NoNodeAvailableException: None of the configured nodes were available: [{XrEPuUd}{XrEPuUliQnaIliQj2dQ7tx}{a5f_4-JNRoSxxv3Y5Yfj8g}{172.20.0.3}{172.20.0.3:9300}{ml.max_open_jobs=10, ml.enabled=true}]",
        "cached": true,
        "className": "com.netflix.conductor.health.ElasticSearchHealthIndicator"
      },
      "healthy": false,
      "errorMessage": {
        "present": true
      }
    }
  ],
  "suppressedHealthResults": [],
  "healthy": false
}
```
