# Configuration

## Workflow
- `db` - set to `sqlserver`  to use with Microsoft Sql Server
- `jdbc.url` - JDBC url to sqlserver (for example `jdbc:sqlserver://sqlserver:1433;database=Conductor;encrypt=false;trustServerCertificate=true;`)
- `jdbc.username` - username for database
- `jdbc.password` - password for database
- `flyway.enabled` - You probably want this to be `true`.
- `flyway.table` - Set to `__migrations`
- `conductor.sqlserver.lock.timeout` - The [lock timeout](https://docs.microsoft.com/en-us/sql/t-sql/statements/set-lock-timeout-transact-sql?view=sql-server-ver15) for queries, in milliseconds. 
- `conductor.sqlserver.processUnacks.intervalMillieconds` Process unacks interval for QueueDAO. If you wanted to,
  you could set this to 0 and write a job to do this on the sqlserver.
- `conductor.sqlserver.processRemoves.intervalSeconds` Interval at which to clean `queue_removed`. When deploying,
  you'll want set this to 0 and configure a job that will do this because the included implementation will clean the entire table. The job should leave around 1000 items in queue_message. See reasoning below. **Default is 0 (disabled)**
- `conductor.sqlserver.queue.sharding.strategy` - Either `local_only` or `shared`. When `shared` all conductor instances will use one big queue (named `GLBL`). When `local_only` the queues will be local to the specific conductor instance. According to the [docs](https://netflix.github.io/conductor/technicaldetails/#more-on-dyno-queues), if the queues are `shared`, it is recommended to enable decider service locking.
- `LOCAL_RACK` - Queue shard name when `conductor.sqlserver.queue.sharding.strategy` is set to `local_only`.
  Max length is 4 characters.

## Lock
- `workflow.decider.locking.server` - set to `SQLSERVER` to use with Microsoft Sql Server
- `workflow.decider.locking.enabled` - enable decider lock
- `workflow.decider.locking.namespace` - a namespace for the locks. Max length is 4 characters.
- `conductor.jetty.server.port` - will be used in the `[reentrant_lock].[holder_id]` to identify a locker, along with the hostname and thread id which will are provided by the runtime. The default is 8080, you should probably not set this property unless you really need to.
- `LOCAL_RACK` - ^

# More info

* When deleting/acking a message from a queue, instead of deleting the row form `queue_message` just the
  message id will be written to `queue_removed` and when accessing a queue a join will be preformed between
  `queue_message` and `queue_removed` to make sure that removed/acked messages are not returned to the user.
  The reason we are doing this is because sqlserver is pretty bad with `DELETE`s on very active table, but
  pretty good with `JOIN`s. This is called a logical delete. Please note that `queue_removed` will grow
  over time and therefore should be cleaned from time to time. We recommend to clean them when `queue_removed`
  reaches around 10k rows.
* You can partition `queue_message` by `queue_shard` which will contain 
whatever you'll configure in `LOCAL_RACK` for possibly better performance.
* Some indexes have `sort_in_tempdb=on`, you can move the tempdb to the memory 
to increase performance
* You will probably want to write jobs to clean out the following tables:
    * `event_execution`
    * `task_scheduled`
    * `task`
    * `workflow_def_to_workflow`
    * `workflow_to_task`
    * `reentrant_lock`
    * `queue_removed` and `queue_message` 
* When cleaning `queue_message` and `queue_removed`, you should not clean the whole table, but
  leave a 1000 or so rows because if the table is too small sqlserver won't use the indexes
  and everything will slow down to a crawl.
* You can set the query timeout from the connection string by adding `queryTimeout=10`
* You can set `conductor.sqlserver.deadlock.retry.max` to the max retries when a deadlock, lock timeout 
  or query timeout occurs.
* You can make `queue_message` and `queue_removed` in memory tables to improve performance
