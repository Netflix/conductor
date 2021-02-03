-- --------------------------------------------------------------------------------------------------------------
-- Migration script V1 for SQLSERVER
-- Table and index names are taken from MySQL
-- --------------------------------------------------------------------------------------------------------------
GO

-- --------------------------------------------------------------------------------------------------------------
-- TAGLES FOR METADATA DAO
-- --------------------------------------------------------------------------------------------------------------

-- meta_event_handler
CREATE TABLE [dbo].[meta_event_handler] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    name VARCHAR(255) NOT NULL,
    event VARCHAR(255) NOT NULL,
    active BIT NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE INDEX event_handler_name_index 
ON dbo.meta_event_handler(name);
GO

CREATE INDEX event_handler_event_index 
ON dbo.meta_event_handler(event);
GO

-- meta_task_def 
CREATE TABLE [dbo].[meta_task_def] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    name VARCHAR(255) NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE UNIQUE INDEX unique_task_def_name
ON dbo.meta_task_def(name);
GO

-- meta_workflow_def
CREATE TABLE [dbo].[meta_workflow_def] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    name VARCHAR(255) NOT NULL,
    version INT NOT NULL,
    latest_version INT DEFAULT 0 NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE UNIQUE INDEX unique_name_version 
ON dbo.meta_workflow_def(name, version);
GO

CREATE INDEX workflow_def_name_index
ON dbo.meta_workflow_def(name);
GO

-- --------------------------------------------------------------------------------------------------------------
-- TAGLES FOR EXECUTION DAO
-- --------------------------------------------------------------------------------------------------------------

-- event_execution 
CREATE TABLE [dbo].[event_execution] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,    
    event_handler_name VARCHAR(255) NOT NULL,
    event_name VARCHAR(255) NOT NULL,
    message_id VARCHAR(255) NOT NULL,
    execution_id VARCHAR(255) NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE UNIQUE INDEX unique_event_execution
ON dbo.event_execution(event_handler_name, event_name, execution_id);
GO

-- poll_data
CREATE TABLE [dbo].[poll_data] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,    
    queue_name VARCHAR(255) NOT NULL,
    domain VARCHAR(255) NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE UNIQUE INDEX unique_poll_data
ON dbo.poll_data(queue_name, domain);
GO

CREATE INDEX queue_name
ON dbo.poll_data(queue_name);
GO

-- task_scheduled 
CREATE TABLE [dbo].[task_scheduled] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,    
    workflow_id VARCHAR(255) NOT NULL,
    task_key VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL
);
GO

CREATE UNIQUE INDEX unique_workflow_id_task_key
ON dbo.task_scheduled(workflow_id, task_key);
GO

-- task_in_progress
CREATE TABLE [dbo].[task_in_progress] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,    
    task_def_name VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    workflow_id VARCHAR(255) NOT NULL,
    in_progress_status BIT DEFAULT 0 NOT NULL
);
GO

CREATE UNIQUE INDEX unique_task_def_task_id1
ON dbo.task_in_progress(task_def_name, task_id);
GO

-- task
CREATE TABLE [dbo].[task] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,    
    task_id VARCHAR(255) NOT NULL,
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE UNIQUE INDEX unique_task_id
ON dbo.task(task_id);
GO

-- workflow
CREATE TABLE [dbo].[workflow] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,    
    workflow_id VARCHAR(255) NOT NULL,
    correlation_id VARCHAR(255),
    json_data NVARCHAR(MAX) NOT NULL
);
GO

CREATE UNIQUE INDEX unique_workflow_id
ON dbo.workflow(workflow_id);
GO

CREATE INDEX workflow_corr_id_index
ON dbo.workflow(correlation_id);
GO

-- workflow_def_to_workflow
CREATE TABLE [dbo].[workflow_def_to_workflow] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    workflow_def VARCHAR(255) NOT NULL,
    date_str INT NOT NULL,
    workflow_id VARCHAR(255) NOT NULL,
);
GO

CREATE UNIQUE INDEX unique_workflow_def_date_str
ON dbo.workflow_def_to_workflow(workflow_def, date_str, workflow_id);
GO

-- workflow_pending
CREATE TABLE [dbo].[workflow_pending] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    workflow_type VARCHAR(255) NOT NULL,
    workflow_id VARCHAR(255) NOT NULL
);
GO

CREATE UNIQUE INDEX unique_workflow_type_workflow_id 
ON dbo.workflow_pending(workflow_type, workflow_id) WITH IGNORE_DUP_KEY;
GO

CREATE INDEX workflow_type_index
ON dbo.workflow_pending(workflow_type);
GO

-- workflow_to_task
CREATE TABLE [dbo].[workflow_to_task] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    modified_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    workflow_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL
);
GO

CREATE UNIQUE INDEX unique_workflow_to_task_id
ON dbo.workflow_to_task(workflow_id, task_id);
GO

CREATE INDEX workflow_id_index
ON dbo.workflow_to_task(workflow_id);
GO

-- --------------------------------------------------------------------------------------------------------------
-- TAGLES FOR QUEUE DAO
-- --------------------------------------------------------------------------------------------------------------

-- queue 
CREATE TABLE [dbo].[queue] (
    id INT PRIMARY KEY IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
);
GO

CREATE UNIQUE INDEX unique_queue_name
ON dbo.queue(queue_name) WITH IGNORE_DUP_KEY;
GO

-- queue_message
CREATE TABLE [dbo].[queue_message] (
    id INT IDENTITY(1, 1),
    created_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    deliver_on DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    message_id VARCHAR(255) NOT NULL,
    priority TINYINT DEFAULT 0,
    popped BIT DEFAULT 0,
    offset_time_seconds VARCHAR(64),
    payload NVARCHAR(MAX)
);
GO

CREATE UNIQUE CLUSTERED INDEX UIX_queue_message
ON dbo.queue_message( id asc )
with( allow_row_locks = on, allow_page_locks=off  );
GO

CREATE UNIQUE INDEX unique_queue_name_message_id
ON dbo.queue_message(queue_name, message_id)
with( allow_row_locks = on, allow_page_locks=off  );
GO

CREATE INDEX combo_queue_message
ON dbo.queue_message(queue_name, popped, deliver_on, created_on, priority)
with( allow_row_locks = on, allow_page_locks=off );
GO

-- --------------------------------------------------------------------------------------------------------------
-- TAGLES FOR LOCK DAO
-- --------------------------------------------------------------------------------------------------------------

-- reentrant_lock 
CREATE TABLE [dbo].[reentrant_lock] (
    lock_id VARCHAR(255) PRIMARY KEY NOT NULL,
    holder_id VARCHAR(255) NOT NULL,
    expire_time DATETIME2 DEFAULT SYSDATETIME() NOT NULL
);
GO
