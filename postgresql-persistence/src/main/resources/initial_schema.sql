create table log4j_logs
(
    id       bigserial primary key,
    log_time timestamp,
    logger   text,
    level    text,
    owner    text,
    hostname text,
    fromhost text,
    message  text,
    stack    text,
    alloc_id text,
    trace_id text,
    span_id text
);
create index log4j_logs_log_time_idx on log4j_logs (log_time);

-- --------------------------------------------------------------------------------------------------------------
-- schema for metadata dao
-- --------------------------------------------------------------------------------------------------------------
create table meta_config
(
    name  varchar(255) primary key,
    value text
);
insert into meta_config
values ('log4j_logger_io_grpc_netty', 'INFO');
insert into meta_config
values ('log4j_logger_org_apache_http', 'INFO');
insert into meta_config
values ('log4j_logger_org_eclipse_jetty', 'INFO');
insert into meta_config
values ('log4j_logger_com_zaxxer_hikari', 'INFO');
insert into meta_config
values ('log4j_logger_com_jayway_jsonpath_internal_path_CompiledPath', 'OFF');
insert into meta_config
values ('log4j_logger_com_netflix_conductor_aurora', 'INFO');
insert into meta_config
values ('log4j_logger_com_netflix_conductor_core_events_shotgun', 'DEBUG');
insert into meta_config
values ('log4j_logger_com_netflix_conductor_core_events_EventProcessor', 'DEBUG');
insert into meta_config
values ('log4j_logger_com_netflix_conductor_core_execution_WorkflowSweeper', 'INFO');
insert into meta_config
values ('log4j_logger_com_netflix_conductor_core_execution_DeciderService', 'INFO');
insert into meta_config
values ('log4j_logger_com_netflix_conductor_core_execution_WorkflowExecutor', 'INFO');
insert into meta_config
values ('log4j_logger_com_netflix_conductor_contribs_http', 'INFO');
insert into meta_config
values ('log4j_logger_com_netflix_conductor_contribs_queue_shotgun', 'DEBUG');
insert into meta_config
values ('log4j_logger_com_netflix_conductor_core_execution_tasks_SystemTaskWorkerCoordinator', 'INFO');
commit;

create table meta_task_def
(
    id          bigserial primary key,
    created_on  timestamp    not null default now(),
    modified_on timestamp    not null default now(),
    name        varchar(255) not null,
    json_data   text         not null
);

create table meta_workflow_def
(
    id             bigserial primary key,
    created_on     timestamp    not null default now(),
    modified_on    timestamp    not null default now(),
    name           varchar(255) not null,
    version        int          not null,
    latest_version int          not null default 0,
    json_data      text         not null
);

create table meta_event_handler
(
    id          bigserial primary key,
    created_on  timestamp    not null default now(),
    modified_on timestamp    not null default now(),
    name        varchar(255) not null,
    event       varchar(255) not null,
    active      boolean      not null,
    json_data   text         not null
);

CREATE SEQUENCE meta_error_registry_id_seq;

CREATE TABLE meta_error_registry
(
    id INTEGER DEFAULT nextval('meta_error_registry_id_seq'::regclass) NOT NULL,
    error_code TEXT NOT NULL,
    lookup TEXT NOT NULL,
    workflow_name TEXT,
    general_message TEXT,
    root_cause TEXT,
    resolution TEXT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (error_code),
    UNIQUE (lookup, workflow_name)
);

CREATE sequence meta_priority_id_seq START WITH 1 increment BY 1 no maxvalue no minvalue cache 20 no cycle;

CREATE TABLE meta_priority
(
    id BIGINT DEFAULT nextval('meta_priority_id_seq'::regclass) NOT NULL,
    created_on timestamp not null default now(),
    modified_on timestamp not null default now(),
    min_priority INTEGER NOT NULL,
    max_priority INTEGER NOT NULL,
    name CHARACTER VARYING(255) NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO meta_priority (min_priority, max_priority, name, value)
VALUES (1, 1, 'hybrik-transcode-server', 'transcode-hybrik-lightning');
INSERT INTO meta_priority (min_priority, max_priority, name, value)
VALUES (2, 2, 'hybrik-transcode-server', 'transcode-hybrik-urgent');
INSERT INTO meta_priority (min_priority, max_priority, name, value)
VALUES (3, 4, 'hybrik-transcode-server', 'transcode-hybrik-high');
INSERT INTO meta_priority (min_priority, max_priority, name, value)
VALUES (5, 6, 'hybrik-transcode-server', 'transcode-hybrik-medium');
INSERT INTO meta_priority (min_priority, max_priority, name, value)
VALUES (7, 8, 'hybrik-transcode-server', 'transcode-hybrik');
INSERT INTO meta_priority (min_priority, max_priority, name, value)
VALUES (9, 10, 'hybrik-transcode-server', 'transcode-hybrik-low');


-- ----------------------------------------------------------------------------------------------------------------
-- schema for execution dao
-- --------------------------------------------------------------------------------------------------------------
create table workflow
(
    id                 bigserial primary key,
    created_on         timestamp    not null default now(),
    modified_on        timestamp    not null default now(),
    start_time         timestamp,
    end_time           timestamp,
    parent_workflow_id varchar(255),
    workflow_id        varchar(255) not null,
    workflow_type      varchar(255) not null,
    workflow_status    varchar(255) not null,
    date_str           integer      not null,
    json_data          text         not null,
    input              text,
    output             text,
    correlation_id     text,
    tags               text[]
);
create unique index workflow_workflow_id on workflow (workflow_id);
alter table workflow
    add constraint workflow_workflow_id unique using index workflow_workflow_id;
create index workflow_type_status_date on workflow (workflow_type, workflow_status, date_str);
create index workflow_parent_workflow_id on workflow (parent_workflow_id);
create index workflow_start_time on workflow (start_time);
create index workflow_end_time on workflow (end_time);
create index workflow_type_time on workflow (workflow_type, start_time);

create table task_in_progress
(
    id            bigserial primary key,
    created_on    timestamp    not null default now(),
    modified_on   timestamp    not null default now(),
    in_progress   boolean      not null default false,
    task_def_name varchar(255) not null,
    task_id       varchar(255) not null,
    workflow_id   varchar(255) not null
);
create unique index task_in_progress_fields on task_in_progress (task_def_name, task_id);
alter table task_in_progress
    add constraint task_in_progress_fields unique using index task_in_progress_fields;
create index task_in_progress_wfid on task_in_progress (workflow_id);

create table task_rate_limit
(
    id            bigserial primary key,
    created_on    timestamp    not null default now(),
    expires_on    timestamp    not null,
    task_def_name varchar(255) not null
);
create index task_rate_limit_name_created on task_rate_limit (task_def_name, created_on);

create table task
(
    id           bigserial primary key,
    created_on   timestamp    not null default now(),
    modified_on  timestamp    not null default now(),
    task_id      varchar(255) not null,
    task_type    varchar(255) not null,
    task_refname varchar(255) not null,
    task_status  varchar(255) not null,
    workflow_id  varchar(255) not null,
    json_data    text         not null,
    input        text,
    output       text,
    start_time   timestamp,
    end_time     timestamp
);
create unique index task_task_id on task (task_id);
alter table task
    add constraint task_task_id unique using index task_task_id;
create index task_type_status on task (task_type, task_status);
create index task_workflow_id on task (workflow_id);
create index task_type_time on task (task_type, start_time);

create table task_scheduled
(
    id          bigserial primary key,
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
    id         bigserial primary key,
    created_on timestamp    not null default now(),
    task_id    varchar(255) not null,
    log        text         not null
);
create index task_log_task_id on task_log (task_id);
alter table task_log
    add constraint task_log_task_id_fkey foreign key (task_id) references task (task_id) on delete cascade;

create table poll_data
(
    id          bigserial primary key,
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
    id         bigserial primary key,
    created_on timestamp    not null default now(),
    queue_name varchar(255) not null,
    message_id varchar(255) not null,
    receipt    text,
    json_data  text
);
create index event_message_created_on on event_message (created_on);

create table event_execution
(
    id           bigserial primary key,
    created_on   timestamp    not null default now(),
    modified_on  timestamp    not null default now(),
    handler_name varchar(255) not null,
    event_name   varchar(255) not null,
    message_id   varchar(255) not null,
    execution_id varchar(255) not null,
    status       varchar(255) not null,
    subject      varchar(255) not null,
    received_on  timestamp,
    accepted_on  timestamp,
    started_on   timestamp,
    processed_on timestamp
);
create index event_execution_created_on on event_execution (created_on);
create unique index event_execution_fields on event_execution (handler_name, event_name, message_id, execution_id);
alter table event_execution
    add constraint event_execution_fields unique using index event_execution_fields;
create index event_execution_combo ON event_execution(subject, received_on);

create table event_published
(
    id           bigserial primary key,
    created_on   timestamp    not null default now(),
    json_data    text         not null,
    message_id   varchar(255) not null,
    subject      varchar(255) not null,
    published_on timestamp    not null
);
create index event_published_subject_date on event_published (subject, published_on);
create index event_published_created_on on event_published (created_on);

-- --------------------------------------------------------------------------------------------------------------
-- schema for queue dao
-- --------------------------------------------------------------------------------------------------------------
create table queue
(
    id         bigserial primary key,
    created_on timestamp    not null default now(),
    queue_name varchar(255) not null
);
create unique index queue_name on queue (queue_name);
alter table queue
    add constraint queue_name unique using index queue_name;

create table queue_message
(
    id         bigserial primary key,
    queue_name varchar(255) not null,
    message_id varchar(255) not null,
    version    bigint       not null default 0,
    popped     boolean      not null default false,
    unacked    boolean      not null default false,
    deliver_on timestamp,
    unack_on   timestamp,
    payload    text
);
create unique index queue_name_msg on queue_message (queue_name, message_id);
alter table queue_message
    add constraint queue_name_msg unique using index queue_name_msg;
create index queue_message_deliver_on on queue_message (deliver_on);
create index queue_message_unack_on on queue_message (unack_on);
create index queue_message_message_id on queue_message (message_id);
