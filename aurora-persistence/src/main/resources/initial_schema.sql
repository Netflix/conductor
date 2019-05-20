-- --------------------------------------------------------------------------------------------------------------
-- schema for metadata dao
-- --------------------------------------------------------------------------------------------------------------
create table meta_config
(
    name        varchar(255) primary key,
    value       varchar(255)
);

create table meta_task_def
(
    id          serial primary key,
    created_on  timestamp default current_timestamp,
    modified_on timestamp default current_timestamp,
    name        varchar(255) not null,
    json_data   text         not null
);

create table meta_workflow_def
(
    id             serial primary key,
    created_on     timestamp             default current_timestamp,
    modified_on    timestamp             default current_timestamp,
    name           varchar(255) not null,
    version        int          not null,
    latest_version int          not null default 0,
    json_data      text         not null
);

create table meta_event_handler
(
    id          serial primary key,
    created_on  timestamp default current_timestamp,
    modified_on timestamp default current_timestamp,
    name        varchar(255) not null,
    event       varchar(255) not null,
    active      boolean      not null,
    json_data   text         not null
);

-- -- --------------------------------------------------------------------------------------------------------------
-- -- schema for execution dao
-- -- --------------------------------------------------------------------------------------------------------------
--
-- create table event_execution
-- (
--     id          serial       not null,
--     created_on         timestamp default current_timestamp,
--     modified_on        timestamp default current_timestamp,
--     event_handler_name varchar(255) not null,
--     event_name         varchar(255) not null,
--     message_id         varchar(255) not null,
--     execution_id       varchar(255) not null,
--     json_data          mediumtext   not null,
--     primary key (id),
--     unique key unique_event_execution (event_handler_name,event_name,message_id)
-- );
--
-- create table poll_data
-- (
--     id          serial       not null,
--     created_on  timestamp default current_timestamp,
--     modified_on timestamp default current_timestamp,
--     queue_name  varchar(255) not null,
--     domain      varchar(255) not null,
--     json_data   mediumtext   not null,
--     primary key (id),
--     unique key unique_poll_data (queue_name, domain),
--     key (queue_name
-- )
--     );
--
-- create table task_scheduled
-- (
--     id          int(11) unsigned not null auto_increment,
--     created_on  timestamp default current_timestamp,
--     modified_on timestamp default current_timestamp,
--     workflow_id varchar(255) not null,
--     task_key    varchar(255) not null,
--     task_id     varchar(255) not null,
--     primary key (id),
--     unique key unique_workflow_id_task_key (workflow_id,task_key)
-- );
--
-- create table task_in_progress
-- (
--     id                 int(11) unsigned not null auto_increment,
--     created_on         timestamp             default current_timestamp,
--     modified_on        timestamp             default current_timestamp,
--     task_def_name      varchar(255) not null,
--     task_id            varchar(255) not null,
--     workflow_id        varchar(255) not null,
--     in_progress_status boolean      not null default false,
--     primary key (id),
--     unique key unique_task_def_task_id1 (task_def_name,task_id)
-- );
--
-- create table task
-- (
--     id          int(11) unsigned not null auto_increment,
--     created_on  timestamp default current_timestamp,
--     modified_on timestamp default current_timestamp,
--     task_id     varchar(255) not null,
--     json_data   mediumtext   not null,
--     primary key (id),
--     unique key unique_task_id (task_id)
-- );
--
-- create table workflow
-- (
--     id             int(11) unsigned not null auto_increment,
--     created_on     timestamp default current_timestamp,
--     modified_on    timestamp default current_timestamp,
--     workflow_id    varchar(255) not null,
--     correlation_id varchar(255),
--     json_data      mediumtext   not null,
--     primary key (id),
--     unique key unique_workflow_id (workflow_id)
-- );
--
-- create table workflow_def_to_workflow
-- (
--     id           int(11) unsigned not null auto_increment,
--     created_on   timestamp default current_timestamp,
--     modified_on  timestamp default current_timestamp,
--     workflow_def varchar(255) not null,
--     date_str     integer      not null,
--     workflow_id  varchar(255) not null,
--     primary key (id),
--     unique key unique_workflow_def_date_str (workflow_def,date_str,workflow_id)
-- );
--
-- create table workflow_pending
-- (
--     id            int(11) unsigned not null auto_increment,
--     created_on    timestamp default current_timestamp,
--     modified_on   timestamp default current_timestamp,
--     workflow_type varchar(255) not null,
--     workflow_id   varchar(255) not null,
--     primary key (id),
--     unique key unique_workflow_type_workflow_id (workflow_type,workflow_id),
--     key           workflow_type_index(workflow_type)
-- );
--
-- create table workflow_to_task
-- (
--     id          int(11) unsigned not null auto_increment,
--     created_on  timestamp default current_timestamp,
--     modified_on timestamp default current_timestamp,
--     workflow_id varchar(255) not null,
--     task_id     varchar(255) not null,
--     primary key (id),
--     unique key unique_workflow_to_task_id (workflow_id,task_id),
--     key         workflow_id_index(workflow_id)
-- );
--
-- -- --------------------------------------------------------------------------------------------------------------
-- -- schema for queue dao
-- -- --------------------------------------------------------------------------------------------------------------
--
-- create table queue
-- (
--     id         int(11) unsigned not null auto_increment,
--     created_on timestamp default current_timestamp,
--     queue_name varchar(255) not null,
--     primary key (id),
--     unique key unique_queue_name (queue_name)
-- );
--
-- create table queue_message
-- (
--     id                  int(11) unsigned not null auto_increment,
--     created_on          timestamp    not null default current_timestamp,
--     deliver_on          timestamp             default current_timestamp,
--     queue_name          varchar(255) not null,
--     message_id          varchar(255) not null,
--     popped              boolean               default false,
--     offset_time_seconds long,
--     payload             mediumtext,
--     primary key (id),
--     unique key unique_queue_name_message_id (queue_name,message_id),
--     key                 combo_queue_message(queue_name, popped, deliver_on, created_on)
-- );
