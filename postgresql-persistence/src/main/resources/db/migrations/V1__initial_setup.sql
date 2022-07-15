create table if not exists log4j_logs
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
    span_id  text
);
create index if not exists log4j_logs_log_time_idx on log4j_logs (log_time);

create table if not exists meta_config
(
    name  varchar(255) primary key,
    value text
);

create table if not exists meta_task_def
(
    id          bigserial primary key,
    created_on  timestamp    not null default now(),
    modified_on timestamp    not null default now(),
    name        varchar(255) not null,
    json_data   text         not null
);

create table if not exists meta_workflow_def
(
    id             bigserial primary key,
    created_on     timestamp    not null default now(),
    modified_on    timestamp    not null default now(),
    name           varchar(255) not null,
    version        int          not null,
    latest_version int          not null default 0,
    json_data      text         not null
);

create table if not exists meta_event_handler
(
    id          bigserial primary key,
    created_on  timestamp    not null default now(),
    modified_on timestamp    not null default now(),
    name        varchar(255) not null,
    event       varchar(255) not null,
    active      boolean      not null,
    json_data   text         not null
);

CREATE SEQUENCE IF NOT EXISTS meta_error_registry_id_seq;

create table if not exists meta_error_registry
(
    id              INTEGER DEFAULT nextval('meta_error_registry_id_seq'::regclass) NOT NULL,
    error_code      TEXT                                                            NOT NULL,
    lookup          TEXT                                                            NOT NULL,
    workflow_name   TEXT,
    general_message TEXT,
    root_cause      TEXT,
    resolution      TEXT                                                            NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (error_code),
    UNIQUE (lookup, workflow_name)
);

CREATE SEQUENCE IF NOT EXISTS meta_priority_id_seq START WITH 1 increment BY 1 no maxvalue no minvalue cache 20 no cycle;

create table if not exists meta_priority
(
    id           BIGINT                          DEFAULT nextval('meta_priority_id_seq'::regclass) NOT NULL,
    created_on   timestamp              not null default now(),
    modified_on  timestamp              not null default now(),
    min_priority INTEGER                NOT NULL,
    max_priority INTEGER                NOT NULL,
    name         CHARACTER VARYING(255) NOT NULL,
    value        TEXT                   NOT NULL,
    PRIMARY KEY (id)
);

create table if not exists workflow
(
    id bigserial
        primary key,
    created_on timestamp default now() not null,
    modified_on timestamp default now() not null,
    start_time timestamp,
    end_time timestamp,
    parent_workflow_id varchar(255),
    workflow_id varchar(255) not null
        constraint workflow_workflow_id
            unique,
    workflow_type varchar(255) not null,
    workflow_status varchar(255) not null,
    date_str integer not null,
    json_data text not null,
    input text,
    output text,
    correlation_id text,
    tags text[]
);
create unique index if not exists workflow_workflow_id on workflow (workflow_id);

create table if not exists task_in_progress
(
    id bigserial
        primary key,
    created_on timestamp default now() not null,
    modified_on timestamp default now() not null,
    in_progress boolean default false not null,
    task_def_name varchar(255) not null,
    task_id varchar(255) not null,
    workflow_id varchar(255) not null,
    constraint task_in_progress_fields
        unique (task_def_name, task_id)
);
create unique index if not exists task_in_progress_fields on task_in_progress (task_def_name, task_id);
create index if not exists task_in_progress_wfid on task_in_progress (workflow_id);

create table if not exists task_rate_limit
(
    id            bigserial primary key,
    created_on    timestamp    not null default now(),
    expires_on    timestamp    not null,
    task_def_name varchar(255) not null
);
create index if not exists task_rate_limit_name_created on task_rate_limit (task_def_name, created_on);

create table if not exists task
(
    id bigserial
        primary key,
    created_on timestamp default now() not null,
    modified_on timestamp default now() not null,
    task_id varchar(255) not null
        constraint task_task_id
            unique,
    task_type varchar(255) not null,
    task_refname varchar(255) not null,
    task_status varchar(255) not null,
    workflow_id varchar(255) not null,
    json_data text not null,
    input text,
    output text,
    start_time timestamp,
    end_time timestamp
);
create unique index if not exists task_task_id on task (task_id);
create index if not exists task_type_status on task (task_type, task_status);
create index if not exists task_workflow_id on task (workflow_id);
create index if not exists task_type_time on task (task_type, start_time);

create table if not exists task_scheduled
(
    id bigserial
        primary key,
    created_on timestamp default now() not null,
    modified_on timestamp default now() not null,
    workflow_id varchar(255) not null,
    task_key varchar(255) not null,
    task_id varchar(255) not null,
    constraint task_scheduled_wf_task
        unique (workflow_id, task_key)
);
create unique index if not exists task_scheduled_wf_task on task_scheduled (workflow_id, task_key);

create table if not exists task_log
(
    id bigserial
        primary key,
    created_on timestamp default now() not null,
    task_id varchar(255) not null
        references task (task_id)
            on delete cascade,
    log text not null
);
create index if not exists task_log_task_id on task_log (task_id);

create table if not exists poll_data
(
    id bigserial
        primary key,
    created_on timestamp default now() not null,
    modified_on timestamp default now() not null,
    queue_name varchar(255) not null,
    domain varchar(255) not null,
    json_data text not null,
    constraint poll_data_fields
        unique (queue_name, domain)
);
create unique index if not exists poll_data_fields on poll_data (queue_name, domain);

create table if not exists event_message
(
    id         bigserial primary key,
    created_on timestamp    not null default now(),
    queue_name varchar(255) not null,
    message_id varchar(255) not null,
    receipt    text,
    json_data  text
);
create index if not exists event_message_created_on on event_message (created_on);

create table if not exists event_execution
(
    id           bigserial
        primary key,
    created_on   timestamp default now() not null,
    modified_on  timestamp default now() not null,
    handler_name varchar(255)            not null,
    event_name   varchar(255)            not null,
    message_id   varchar(255)            not null,
    execution_id varchar(255)            not null,
    status       varchar(255)            not null,
    subject      varchar(255)            not null,
    received_on  timestamp,
    accepted_on  timestamp,
    started_on   timestamp,
    processed_on timestamp,
    constraint event_execution_fields
        unique (handler_name, event_name, message_id, execution_id)
);
create index if not exists event_execution_combo ON event_execution (subject, received_on);

create table if not exists event_published
(
    id           bigserial primary key,
    created_on   timestamp    not null default now(),
    json_data    text         not null,
    message_id   varchar(255) not null,
    subject      varchar(255) not null,
    published_on timestamp    not null
);
create index if not exists event_published_subject_date on event_published (subject, published_on);
create index if not exists event_published_created_on on event_published (created_on);

-- --------------------------------------------------------------------------------------------------------------
-- schema for queue dao
-- --------------------------------------------------------------------------------------------------------------
create table if not exists queue
(
    id bigserial
        primary key,
    created_on timestamp default now() not null,
    queue_name varchar(255) not null
        constraint queue_name
            unique
);
create unique index if not exists queue_name on queue (queue_name);

create table if not exists queue_message
(
    id bigserial
        primary key,
    queue_name varchar(255) not null,
    message_id varchar(255) not null,
    version bigint default 0 not null,
    popped boolean default false not null,
    unacked boolean default false not null,
    deliver_on timestamp,
    unack_on timestamp,
    payload text,
    priority integer default 0 not null,
    constraint queue_name_msg
        unique (queue_name, message_id)
);
create unique index if not exists queue_name_msg on queue_message (queue_name, message_id);
create index if not exists queue_message_deliver_on on queue_message (deliver_on);
create index if not exists queue_message_unack_on on queue_message (unack_on);
create index if not exists queue_message_message_id on queue_message (message_id);


CREATE SEQUENCE IF NOT EXISTS event_execution_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS event_message_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS event_published_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS log4j_logs_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS meta_error_registry_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS meta_event_handler_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS meta_priority_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 20
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS meta_task_def_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS meta_workflow_def_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS poll_data_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS queue_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS queue_message_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS task_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS task_in_progress_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS task_log_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS task_rate_limit_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS task_scheduled_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS workflow_error_registry_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS workflow_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;



CREATE OR REPLACE FUNCTION public.get_error_details(p_error_str text, p_workflow_name text)
    RETURNS text
    LANGUAGE sql
    STRICT
AS $function$
SELECT error_code || ' - ' || lookup || ' - ' || general_message || ' - ' || resolution FROM (
                                                                                                 SELECT SUBSTRING(p_error_str, lookup) AS matched_txt, *
                                                                                                 FROM meta_error_registry mer
                                                                                                 WHERE (mer.WORKFLOW_NAME = REGEXP_REPLACE(p_workflow_name, '(\.\d+)+','') OR mer.WORKFLOW_NAME IS NULL)
                                                                                             ) AS match_results
WHERE matched_txt IS NOT NULL
ORDER BY WORKFLOW_NAME, LENGTH(matched_txt) DESC
$function$
;

CREATE OR REPLACE FUNCTION public.pg_stat_statements(showtext boolean, OUT userid oid, OUT dbid oid, OUT queryid bigint, OUT query text, OUT calls bigint, OUT total_time double precision, OUT min_time double precision, OUT max_time double precision, OUT mean_time double precision, OUT stddev_time double precision, OUT rows bigint, OUT shared_blks_hit bigint, OUT shared_blks_read bigint, OUT shared_blks_dirtied bigint, OUT shared_blks_written bigint, OUT local_blks_hit bigint, OUT local_blks_read bigint, OUT local_blks_dirtied bigint, OUT local_blks_written bigint, OUT temp_blks_read bigint, OUT temp_blks_written bigint, OUT blk_read_time double precision, OUT blk_write_time double precision)
    RETURNS SETOF record
    LANGUAGE c
    PARALLEL SAFE STRICT
AS '$libdir/pg_stat_statements', $function$pg_stat_statements_1_3$function$
;

CREATE OR REPLACE FUNCTION public.pg_stat_statements_reset()
    RETURNS void
    LANGUAGE c
    PARALLEL SAFE
AS '$libdir/pg_stat_statements', $function$pg_stat_statements_reset$function$
;

CREATE OR REPLACE FUNCTION public.uuid_generate_v1()
    RETURNS uuid
    LANGUAGE c
    PARALLEL SAFE STRICT
AS '$libdir/uuid-ossp', $function$uuid_generate_v1$function$
;

CREATE OR REPLACE FUNCTION public.uuid_generate_v1mc()
    RETURNS uuid
    LANGUAGE c
    PARALLEL SAFE STRICT
AS '$libdir/uuid-ossp', $function$uuid_generate_v1mc$function$
;

CREATE OR REPLACE FUNCTION public.uuid_generate_v3(namespace uuid, name text)
    RETURNS uuid
    LANGUAGE c
    IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/uuid-ossp', $function$uuid_generate_v3$function$
;

CREATE OR REPLACE FUNCTION public.uuid_generate_v4()
    RETURNS uuid
    LANGUAGE c
    PARALLEL SAFE STRICT
AS '$libdir/uuid-ossp', $function$uuid_generate_v4$function$
;

CREATE OR REPLACE FUNCTION public.uuid_generate_v5(namespace uuid, name text)
    RETURNS uuid
    LANGUAGE c
    IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/uuid-ossp', $function$uuid_generate_v5$function$
;

CREATE OR REPLACE FUNCTION public.uuid_nil()
    RETURNS uuid
    LANGUAGE c
    IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/uuid-ossp', $function$uuid_nil$function$
;

CREATE OR REPLACE FUNCTION public.uuid_ns_dns()
    RETURNS uuid
    LANGUAGE c
    IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/uuid-ossp', $function$uuid_ns_dns$function$
;

CREATE OR REPLACE FUNCTION public.uuid_ns_oid()
    RETURNS uuid
    LANGUAGE c
    IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/uuid-ossp', $function$uuid_ns_oid$function$
;

CREATE OR REPLACE FUNCTION public.uuid_ns_url()
    RETURNS uuid
    LANGUAGE c
    IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/uuid-ossp', $function$uuid_ns_url$function$
;

CREATE OR REPLACE FUNCTION public.uuid_ns_x500()
    RETURNS uuid
    LANGUAGE c
    IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/uuid-ossp', $function$uuid_ns_x500$function$
;

CREATE OR REPLACE VIEW public.pg_stat_statements
AS SELECT pg_stat_statements.userid,
          pg_stat_statements.dbid,
          pg_stat_statements.queryid,
          pg_stat_statements.query,
          pg_stat_statements.calls,
          pg_stat_statements.total_time,
          pg_stat_statements.min_time,
          pg_stat_statements.max_time,
          pg_stat_statements.mean_time,
          pg_stat_statements.stddev_time,
          pg_stat_statements.rows,
          pg_stat_statements.shared_blks_hit,
          pg_stat_statements.shared_blks_read,
          pg_stat_statements.shared_blks_dirtied,
          pg_stat_statements.shared_blks_written,
          pg_stat_statements.local_blks_hit,
          pg_stat_statements.local_blks_read,
          pg_stat_statements.local_blks_dirtied,
          pg_stat_statements.local_blks_written,
          pg_stat_statements.temp_blks_read,
          pg_stat_statements.temp_blks_written,
          pg_stat_statements.blk_read_time,
          pg_stat_statements.blk_write_time
   FROM pg_stat_statements(true) pg_stat_statements(userid, dbid, queryid, query, calls, total_time, min_time, max_time, mean_time, stddev_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written, blk_read_time, blk_write_time);


ALTER TABLE workflow ADD IF NOT EXISTS json_data_workflow_ids jsonb;
UPDATE workflow SET json_data_workflow_ids = json_data::jsonb->'workflowIds';
CREATE INDEX IF NOT EXISTS workflow_json_data_workflow_ids_gin_idx ON workflow
    USING gin (json_data_workflow_ids jsonb_path_ops);