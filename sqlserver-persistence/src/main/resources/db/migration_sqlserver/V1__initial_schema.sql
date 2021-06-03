-- --------------------------------------------------------------------------------------------------------------
-- Migration script V1 for SQLSERVER
-- --------------------------------------------------------------------------------------------------------------
GO

-- --------------------------------------------------------------------------------------------------------------
-- TABLES FOR METADATA DAO
-- --------------------------------------------------------------------------------------------------------------

-- meta_event_handler
CREATE TABLE [data].[meta_event_handler] (
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    name VARCHAR(255) NOT NULL,
    event VARCHAR(255) NOT NULL,
    active BIT NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE CLUSTERED INDEX cidx_meta_event_handler
ON [data].[meta_event_handler](created_on);
GO

CREATE INDEX event_handler_name_index 
ON [data].[meta_event_handler](name);
GO

CREATE INDEX event_handler_event_index 
ON [data].[meta_event_handler](event);
GO

-- meta_task_def 
CREATE TABLE [data].[meta_task_def] (
    name VARCHAR(255) PRIMARY KEY,
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);
GO

-- meta_workflow_def
CREATE TABLE [data].[meta_workflow_def] (
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    name VARCHAR(255) NOT NULL,
    version INT NOT NULL,
    latest_version INT DEFAULT 0 NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE CLUSTERED INDEX cidx_meta_workflow_def
ON [data].[meta_workflow_def](created_on);
GO

CREATE UNIQUE INDEX unique_name_version 
ON [data].[meta_workflow_def](name, version)
include(json_data);
GO

-- --------------------------------------------------------------------------------------------------------------
-- TABLES FOR EXECUTION DAO
-- --------------------------------------------------------------------------------------------------------------

-- event_execution 
CREATE TABLE [data].[event_execution] (
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,    
    event_handler_name VARCHAR(255) NOT NULL,
    event_name VARCHAR(255) NOT NULL,
    message_id VARCHAR(255) NOT NULL,
    execution_id VARCHAR(255) NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE CLUSTERED INDEX cidx_event_execution
ON [data].[event_execution](created_on);
GO

CREATE UNIQUE INDEX unique_event_execution
ON [data].[event_execution](event_handler_name, event_name, execution_id);
GO

-- poll_data
CREATE TABLE [data].[poll_data] (
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,    
    queue_name VARCHAR(255) NOT NULL,
    domain VARCHAR(255) NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE UNIQUE CLUSTERED INDEX unique_poll_data
ON [data].[poll_data](queue_name, domain);
GO

-- task_scheduled 
CREATE TABLE [data].[task_scheduled] (
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,    
    workflow_id uniqueidentifier NOT NULL,
    task_key VARCHAR(255) NOT NULL,
    task_id uniqueidentifier NOT NULL
);
GO

CREATE UNIQUE CLUSTERED INDEX unique_workflow_id_task_key
ON [data].[task_scheduled](workflow_id, task_key)
with( allow_row_locks = on, allow_page_locks=off, IGNORE_DUP_KEY=ON, sort_in_tempdb=on, pad_index=on, fillfactor=70 );
GO

CREATE INDEX xi_task_scheduled_task_id
ON [data].[task_scheduled](task_id)
with( allow_row_locks = on, allow_page_locks=off)
GO

-- task_in_progress
CREATE TABLE [data].[task_in_progress] (
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,    
    task_def_name VARCHAR(255) NOT NULL,
    task_id uniqueidentifier NOT NULL,
    workflow_id uniqueidentifier NOT NULL,
    in_progress_status BIT DEFAULT 0 NOT NULL
);
GO

CREATE UNIQUE CLUSTERED INDEX unique_task_def_task_id1
ON [data].[task_in_progress](task_def_name, task_id)
with( allow_row_locks = on, allow_page_locks=off, IGNORE_DUP_KEY=ON, sort_in_tempdb=on, pad_index=on, fillfactor=70);
GO

CREATE INDEX task_in_progress_task_id_idx
ON [data].[task_in_progress](task_id, task_def_name, workflow_id)
with( allow_row_locks = on, allow_page_locks=off, sort_in_tempdb=on);
GO

-- task
CREATE TABLE [data].[task] (
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,    
    task_id uniqueidentifier NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE UNIQUE CLUSTERED INDEX unique_task_id
ON [data].[task](task_id)
with( sort_in_tempdb=on, allow_row_locks = on, allow_page_locks=off, data_compression=page, pad_index=on, fillfactor=70);
GO

-- workflow
CREATE TABLE [data].[workflow] (
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,    
    workflow_id uniqueidentifier NOT NULL,
    correlation_id VARCHAR(255),
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE UNIQUE CLUSTERED INDEX cui_workflow_id
ON [data].[workflow](workflow_id)
with( sort_in_tempdb=on, allow_row_locks = on, allow_page_locks=off,data_compression=page, pad_index=on, fillfactor=70);
GO

-- workflow_def_to_workflow
CREATE TABLE [data].[workflow_def_to_workflow] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    workflow_def VARCHAR(255) NOT NULL,
    date_str INT NOT NULL,
    workflow_id uniqueidentifier NOT NULL,
);
GO

CREATE UNIQUE INDEX unique_workflow_def_date_str
ON [data].[workflow_def_to_workflow](workflow_id, workflow_def, date_str);
GO

-- workflow_pending
CREATE TABLE [data].[workflow_pending] (
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    workflow_type VARCHAR(255) NOT NULL,
    workflow_id uniqueidentifier NOT NULL
);
GO

CREATE UNIQUE CLUSTERED INDEX unique_workflow_type_workflow_id 
ON [data].[workflow_pending](workflow_type, workflow_id) 
WITH (IGNORE_DUP_KEY=ON);
GO

-- workflow_to_task
CREATE TABLE [data].[workflow_to_task] (
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    workflow_id uniqueidentifier NOT NULL,
    task_id uniqueidentifier NOT NULL
);
GO

CREATE UNIQUE CLUSTERED INDEX unique_workflow_to_task_id
ON [data].[workflow_to_task](workflow_id, task_id)
with(pad_index=on,fillfactor=70,sort_in_tempdb=on, online=on, allow_row_locks = on, allow_page_locks=off,IGNORE_DUP_KEY=ON  );
GO

-- --------------------------------------------------------------------------------------------------------------
-- TABLES FOR QUEUE DAO
-- --------------------------------------------------------------------------------------------------------------

-- queue 
CREATE TABLE [data].[queue] (
    queue_name VARCHAR(255) PRIMARY KEY WITH (IGNORE_DUP_KEY=ON),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL
);
GO

-- queue_message
CREATE TABLE [data].[queue_message] (
    id INT IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    deliver_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    queue_shard CHAR(4) NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    message_id uniqueidentifier NOT NULL,
    priority TINYINT DEFAULT 0,
    popped BIT DEFAULT 0,
    offset_time_seconds VARCHAR(64),
    payload NVARCHAR(MAX)
);
GO

CREATE UNIQUE CLUSTERED INDEX cui_queue_message
ON [data].[queue_message]( id asc )
with( allow_row_locks = on, allow_page_locks=off,sort_in_tempdb=on);
GO

CREATE UNIQUE INDEX unique_queue_name_message_id
ON [data].[queue_message](queue_shard, queue_name, message_id)
with( allow_row_locks = on, allow_page_locks=off  );
GO

CREATE INDEX combo_queue_message
ON [data].[queue_message](queue_shard, queue_name, popped, deliver_on)
with( allow_row_locks = on, allow_page_locks=off,sort_in_tempdb=on);
GO

CREATE INDEX xi_popped_deliver_on
ON [data].[queue_message](popped, deliver_on)
with( allow_row_locks = on, allow_page_locks=off  );
GO

-- queue_message
CREATE TABLE [data].[queue_removed] (
    queue_shard CHAR(4) NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    message_id uniqueidentifier NOT NULL,
);
GO

CREATE UNIQUE INDEX unique_queue_shard_name_message_id
ON [data].[queue_removed](queue_shard, queue_name, message_id)
with( allow_row_locks = on, allow_page_locks=off,IGNORE_DUP_KEY=ON);
GO

-- --------------------------------------------------------------------------------------------------------------
-- TABLES FOR LOCK DAO
-- --------------------------------------------------------------------------------------------------------------

-- reentrant_lock 
CREATE TABLE [data].[reentrant_lock] (
    ns CHAR(4) NOT NULL,
    lock_id uniqueidentifier NOT NULL,
    holder_id VARCHAR(255) NOT NULL,
    expire_time DATETIME2 DEFAULT SYSDATETIME() NOT NULL
);
GO

CREATE UNIQUE CLUSTERED INDEX unique_reentrant_lock
ON [data].[reentrant_lock](ns, lock_id)
with( allow_row_locks = on, allow_page_locks=off, IGNORE_DUP_KEY=ON, pad_index=on, fillfactor=70 );
GO

CREATE INDEX ix_reentrant_lock
ON [data].[reentrant_lock](holder_id, lock_id )
with( allow_row_locks = on, allow_page_locks=off );
GO
