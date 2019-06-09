create table log4j_logs
(
    log_time timestamp,
    logger   text,
    level    text,
    owner    text,
    hostname text,
    fromhost text,
    message  text,
    stack    text
);
create index log4j_logs_log_time_idx on log4j_logs (log_time);

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
create table workflow
(
    id                 serial primary key,
    created_on         timestamp    not null default now(),
    modified_on        timestamp    not null default now(),
    start_time         timestamp,
    end_time           timestamp,
    workflow_id        varchar(255) not null,
    workflow_type      varchar(255) not null,
    workflow_status    varchar(255) not null,
    parent_workflow_id varchar(255),
    json_data          text         not null,
    input              text,
    output             text,
    correlation_id     text,
    tags               text[],
    date_str           integer      not null
);
create unique index workflow_workflow_id on workflow (workflow_id);
alter table workflow
    add constraint workflow_workflow_id unique using index workflow_workflow_id;
create index workflow_type_status_date on workflow (workflow_type, workflow_status, date_str);
create index workflow_start_time on workflow (start_time);

create table task_in_progress
(
    id            serial primary key,
    created_on    timestamp    not null default now(),
    modified_on   timestamp    not null default now(),
    in_progress   boolean      not null default false,
    task_def_name varchar(255) not null,
    task_id       varchar(255) not null,
    workflow_id   varchar(255) not null
);
create unique index task_in_progress_fields on task_in_progress (task_def_name, workflow_id);
alter table task_in_progress
    add constraint task_in_progress_fields unique using index task_in_progress_fields;

create index task_in_progress_def_id on task_in_progress (task_def_name, task_id);

create table task
(
    id          serial primary key,
    created_on  timestamp    not null default now(),
    modified_on timestamp    not null default now(),
    task_id     varchar(255) not null,
    task_type   varchar(255) not null,
    task_status varchar(255) not null,
    workflow_id varchar(255) not null,
    json_data   text         not null
);
create unique index task_task_id on task (task_id);
alter table task
    add constraint task_task_id unique using index task_task_id;
create index task_workflow_id on task (workflow_id);

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

create table task_log
(
    id         serial primary key,
    created_on timestamp    not null default now(),
    task_id    varchar(255) not null,
    log        text         not null
);
create index task_log_task_id on task_log (task_id);

create table poll_data
(
    id          serial primary key,
    created_on  timestamp    not null default now(),
    modified_on timestamp    not null default now(),
    queue_name  varchar(255) not null,
    domain      varchar(255) not null,
    json_data   text         not null
);

create unique index poll_data_fields on poll_data (queue_name, domain);
alter table poll_data
    add constraint poll_data_fields unique using index poll_data_fields;

create table event_message
(
    id         serial primary key,
    created_on timestamp    not null default now(),
    queue_name varchar(255) not null,
    message_id varchar(255) not null,
    receipt    text         not null,
    json_data  text         not null
);

create table event_execution
(
    id           serial primary key,
    created_on   timestamp    not null default now(),
    modified_on  timestamp    not null default now(),
    handler_name varchar(255) not null,
    event_name   varchar(255) not null,
    message_id   varchar(255) not null,
    execution_id varchar(255) not null,
    status       varchar(255) not null,
    subject      varchar(255) not null,
    json_data    text         not null,
    received_on  timestamp,
    accepted_on  timestamp,
    started_on   timestamp,
    processed_on timestamp
);
create unique index event_execution_fields on event_execution (handler_name, event_name, message_id, execution_id);
alter table event_execution
    add constraint event_execution_fields unique using index event_execution_fields;

create table event_published
(
    id           serial primary key,
    created_on   timestamp    not null default now(),
    json_data    text         not null,
    message_id   varchar(255) not null,
    subject      varchar(255) not null,
    published_on timestamp    not null
);
create index event_published_subject_date on event_published (subject, published_on);

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
    id         serial primary key,
    queue_name varchar(255) not null,
    message_id varchar(255) not null,
    version    bigint       not null default 0,
    popped     boolean      not null default false,
    deliver_on timestamp,
    popped_on  timestamp,
    unack_on   timestamp,
    payload    text
);
create unique index queue_name_msg on queue_message (queue_name, message_id);
alter table queue_message
    add constraint queue_name_msg unique using index queue_name_msg;
create index queue_name_combo on queue_message (queue_name, popped, deliver_on);

