-- --------------------------------------------------------------------------------------------------------------
-- schema for metadata dao
-- --------------------------------------------------------------------------------------------------------------
create table meta_config
(
    name  varchar(255) primary key,
    value varchar(255)
);

create table meta_task_def
(
    id          serial primary key,
    created_on  timestamp    not null default now(),
    modified_on timestamp    not null default now(),
    name        varchar(255) not null,
    json_data   text         not null
);

create table meta_workflow_def
(
    id             serial primary key,
    created_on     timestamp    not null default now(),
    modified_on    timestamp    not null default now(),
    name           varchar(255) not null,
    version        int          not null,
    latest_version int          not null default 0,
    json_data      text         not null
);

create table meta_event_handler
(
    id          serial primary key,
    created_on  timestamp    not null default now(),
    modified_on timestamp    not null default now(),
    name        varchar(255) not null,
    event       varchar(255) not null,
    active      boolean      not null,
    json_data   text         not null
);

-- ----------------------------------------------------------------------------------------------------------------
-- schema for execution dao
-- --------------------------------------------------------------------------------------------------------------
create table task_in_progress
(
    id                 serial primary key,
    created_on         timestamp    not null default now(),
    modified_on        timestamp    not null default now(),
    in_progress_status boolean      not null default false,
    task_def_name      varchar(255) not null,
    task_id            varchar(255) not null,
    workflow_id        varchar(255) not null
);
create unique index task_in_progress_fields on task_in_progress (task_def_name, workflow_id);

create table task
(
    id          serial primary key,
    created_on  timestamp    not null default now(),
    modified_on timestamp    not null default now(),
    task_id     varchar(255) not null,
    json_data   text         not null
);
create unique index task_task_id on task (task_id);
alter table task
    add constraint task_task_id unique using index task_task_id;

create table workflow
(
    id             serial primary key,
    created_on     timestamp    not null default now(),
    modified_on    timestamp    not null default now(),
    workflow_id    varchar(255) not null,
    json_data      text         not null,
    correlation_id text
);
create unique index workflow_workflow_id on workflow (workflow_id);
alter table workflow
    add constraint workflow_workflow_id unique using index workflow_workflow_id;

create table task_scheduled
(
    id          serial primary key,
    created_on  timestamp    not null default now(),
    modified_on timestamp    not null default now(),
    workflow_id varchar(255) not null,
    task_key    varchar(255) not null,
    task_id     varchar(255) not null
);
create unique index task_scheduled_wf_task on task_scheduled (workflow_id, task_key);
alter table task_scheduled
    add constraint task_scheduled_wf_task unique using index task_scheduled_wf_task;


create table workflow_def_to_workflow
(
    id           serial primary key,
    created_on   timestamp    not null default now(),
    modified_on  timestamp    not null default now(),
    workflow_def varchar(255) not null,
    workflow_id  varchar(255) not null,
    date_str     integer      not null
);
create unique index workflow_def_to_workflow_fields on workflow_def_to_workflow (workflow_def, date_str, workflow_id);
alter table workflow_def_to_workflow
    add constraint workflow_def_to_workflow_fields unique using index workflow_def_to_workflow_fields;


create table workflow_pending
(
    id            serial primary key,
    created_on    timestamp    not null default now(),
    modified_on   timestamp    not null default now(),
    workflow_type varchar(255) not null,
    workflow_id   varchar(255) not null
);
create unique index workflow_pending_fields on workflow_pending (workflow_type, workflow_id);
alter table workflow_pending
    add constraint workflow_pending_fields unique using index workflow_pending_fields;


create table workflow_to_task
(
    id          serial primary key,
    created_on  timestamp    not null default now(),
    modified_on timestamp    not null default now(),
    workflow_id varchar(255) not null,
    task_id     varchar(255) not null
);
create unique index workflow_to_task_fields on workflow_to_task (workflow_id, task_id);
alter table workflow_to_task
    add constraint workflow_to_task_fields unique using index workflow_to_task_fields;


/*
create table event_execution
(
    id                 serial primary key,
    created_on         timestamp default now(),
    modified_on        timestamp default now(),
    event_handler_name varchar(255) not null,
    event_name         varchar(255) not null,
    message_id         varchar(255) not null,
    execution_id       varchar(255) not null,
    json_data          text         not null
--     unique key unique_event_execution (event_handler_name,event_name,message_id)
);

create table event_published
(
    id          serial primary key,
    created_on  timestamp default now(),
    modified_on timestamp default now(),

    json_data   text not null
--     unique key unique_event_execution (event_handler_name,event_name,message_id)
);

create table poll_data
(
    id          serial primary key,
    created_on  timestamp default now(),
    modified_on timestamp default now(),
    queue_name  varchar(255) not null,
    domain      varchar(255) not null,
    json_data   text         not null
--     unique key unique_poll_data (queue_name, domain),
--     key (queue_name)
);

*/
-- --------------------------------------------------------------------------------------------------------------
-- schema for queue dao
-- --------------------------------------------------------------------------------------------------------------

create table queue
(
    id         serial primary key,
    created_on timestamp    not null default now(),
    queue_name varchar(255) not null
);
create unique index queue_name on queue (queue_name);
alter table queue
    add constraint queue_name unique using index queue_name;

create table queue_message
(
    id                  serial primary key,
    created_on          timestamp    not null default now(),
    deliver_on          timestamp    not null default now(),
    popped              boolean      not null default false,
    queue_name          varchar(255) not null,
    message_id          varchar(255) not null,
    offset_time_seconds bigint,
    payload             text
);
create index queue_name_combo on queue_message (queue_name, popped, deliver_on, created_on);
create unique index queue_name_msg on queue_message (queue_name, message_id);
alter table queue_message
    add constraint queue_name_msg unique using index queue_name_msg;

