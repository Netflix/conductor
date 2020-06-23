# Redis Health Check 

This module is optional and depends on `redis-persistence`.

## Build

### Gradle Configuration

Modify the following files

[settings.gradle](https://github.com/Netflix/conductor/blob/master/settings.gradle)

```diff
@@ -3,6 +3,7 @@ rootProject.name='conductor'
 include 'client','common','contribs','core', 'es5-persistence','jersey', 'postgres-persistence', 'zookeeper-lock', 'redis-lock'
 include 'cassandra-persistence', 'mysql-persistence', 'redis-persistence','server','test-harness','ui'
 include 'grpc', 'grpc-server', 'grpc-client'
+include 'redis-health'

 rootProject.children.each {it.name="conductor-${it.name}"}

```

[server/build.gradle](https://github.com/Netflix/conductor/blob/master/server/build.gradle)

```diff
@@ -38,6 +38,7 @@ dependencies {

+   compile project(':conductor-redis-health')

    compile "com.netflix.runtime:health-guice:${revHealth}"
```
 
Delete all dependencies.lock files of all modules and then run at the root folder of the project: 

```
./gradlew generateLock saveLock -PdependencyLock.includeTransitives=true
```

Run Build along with the tests

```
./gradlew conductor:redis-health build
```

## Configuration

In the `.properties` file of conductor `server` you must add the following configurations.

* Add the RedisHealthModule module. If you have several modules, separate them with a comma.
```
conductor.additional.modules=com.netflix.conductor.health.RedisHealthModule
```

* Health Indicator can be configured through HealthAggregatorConfiguration. The following attributes can be added to the properties file
```
health.aggregator.cacheHealthIndicators=true
health.aggregator.cacheIntervalInMillis=5000
health.aggregator.aggregatorWaitIntervalInMillis=1000
```

### Usage

Request: HTTP GET /api/health


* If Redis is reachable

Response: HTTP 200
```json
{
  "healthResults": [
    {
      "details": {
        "cached": true,
        "className": "com.netflix.conductor.health.RedisHealthIndicator"
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

* If Redis is unreachable

Response: HTTP 200
```json
{
  "healthResults": [
    {
      "details": {
        "error": "com.netflix.dyno.connectionpool.exception.PoolOfflineException: PoolOfflineException: [host=Host [hostname=UNKNOWN, ipAddress=UNKNOWN, port=0, rack: UNKNOWN, datacenter: UNKNOW, status: Down, hashtag=null, password=null], latency=0(0), attempts=0]host pool is offline and no Racks available for fallback",
        "cached": true,
        "className": "com.netflix.conductor.health.RedisHealthIndicator"
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
