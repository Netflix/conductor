# Configuration

## Workflow
- `db` - set to `sqlserver`  to use with Microsoft Sql Server
- `jdbc.url` - JDBC url to sqlserver (for example `jdbc:sqlserver://sqlserver:1433;database=Conductor;encrypt=false;trustServerCertificate=true;`)
- `jdbc.username` - username for database
- `jdbc.password` - password for database
- `flyway.enabled` - You probably want this to be `true` unless you have multiple conductor instances on the same DB, then only one of them should have this enabled.
- `flyway.table` - Set to `__migrations`
- `conductor.sqlserver.queue.sharding.strategy` - Either `local_only` or `shared`. When `shared` all conductor instances will use one big queue. When `local_only` the queues will be local to the specific conductor instance. According to the [docs](https://netflix.github.io/conductor/technicaldetails/#more-on-dyno-queues), if the queues are `shared`, it is recommended to enable decider service locking.
- `LOCAL_RACK` - Queue suffix when `conductor.sqlserver.queue.sharding.strategy` is set to `local_only`.

## Lock
- `workflow.decider.locking.server` - set to `SQLSERVER` to use with Microsoft Sql Server
- `workflow.decider.locking.enabled` - enable decider lock
- `workflow.decider.locking.namespace` - a prefix for the lockId
- `conductor.jetty.server.port` - will be used in the `[reentrant_lock].[holder_id]` to identify a locker, along with the hostname and thread id which will are provided by the runtime. The default is 8080, you should probably not set this property unless you really need to.
- `LOCAL_RACK` - ^
