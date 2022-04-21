CREATE SEQUENCE public.event_execution_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.event_message_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.event_published_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.log4j_logs_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.meta_error_registry_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.meta_event_handler_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.meta_priority_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 20
	NO CYCLE;

CREATE SEQUENCE public.meta_task_def_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.meta_workflow_def_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.poll_data_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.queue_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.queue_message_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.task_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.task_in_progress_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.task_log_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.task_rate_limit_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.task_scheduled_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.workflow_error_registry_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE SEQUENCE public.workflow_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
	CACHE 1
	NO CYCLE;

CREATE TABLE public.event_execution (
                                        id bigserial NOT NULL,
                                        created_on timestamp NOT NULL DEFAULT now(),
                                        modified_on timestamp NOT NULL DEFAULT now(),
                                        handler_name varchar(255) NOT NULL,
                                        event_name varchar(255) NOT NULL,
                                        message_id varchar(255) NOT NULL,
                                        execution_id varchar(255) NOT NULL,
                                        status varchar(255) NOT NULL,
                                        subject varchar(255) NOT NULL,
                                        received_on timestamp NULL,
                                        accepted_on timestamp NULL,
                                        started_on timestamp NULL,
                                        processed_on timestamp NULL,
                                        CONSTRAINT event_execution_fields UNIQUE (handler_name, event_name, message_id, execution_id),
                                        CONSTRAINT event_execution_pkey PRIMARY KEY (id)
);
CREATE INDEX event_execution_combo ON public.event_execution USING btree (subject, received_on);
CREATE INDEX event_execution_created_on ON public.event_execution USING btree (created_on);

CREATE TABLE public.event_message (
                                      id bigserial NOT NULL,
                                      created_on timestamp NOT NULL DEFAULT now(),
                                      queue_name varchar(255) NOT NULL,
                                      message_id varchar(255) NOT NULL,
                                      receipt text NULL,
                                      json_data text NULL,
                                      CONSTRAINT event_message_pkey PRIMARY KEY (id)
);
CREATE INDEX event_message_created_on ON public.event_message USING btree (created_on);

CREATE TABLE public.event_published (
                                        id bigserial NOT NULL,
                                        created_on timestamp NOT NULL DEFAULT now(),
                                        json_data text NOT NULL,
                                        message_id varchar(255) NOT NULL,
                                        subject varchar(255) NOT NULL,
                                        published_on timestamp NOT NULL,
                                        CONSTRAINT event_published_pkey PRIMARY KEY (id)
);
CREATE INDEX event_published_created_on ON public.event_published USING btree (created_on);
CREATE INDEX event_published_subject_date ON public.event_published USING btree (subject, published_on);

CREATE TABLE public.log4j_logs (
                                   id bigserial NOT NULL,
                                   log_time timestamp NULL,
                                   logger text NULL,
                                   "level" text NULL,
                                   "owner" text NULL,
                                   hostname text NULL,
                                   fromhost text NULL,
                                   message text NULL,
                                   stack text NULL,
                                   alloc_id text NULL,
                                   trace_id text NULL,
                                   span_id text NULL,
                                   CONSTRAINT log4j_logs_pkey PRIMARY KEY (id)
);
CREATE INDEX log4j_logs_log_time_idx ON public.log4j_logs USING btree (log_time);

CREATE TABLE public.meta_config (
                                    "name" varchar(255) NOT NULL,
                                    value text NULL,
                                    CONSTRAINT meta_config_pkey PRIMARY KEY (name)
);

CREATE TABLE public.meta_error_registry (
                                            id serial4 NOT NULL,
                                            error_code text NOT NULL,
                                            lookup text NOT NULL,
                                            workflow_name text NULL,
                                            general_message text NULL,
                                            root_cause text NULL,
                                            resolution text NOT NULL,
                                            isrequiredinreporting bool NOT NULL DEFAULT true,
                                            CONSTRAINT meta_error_registry_error_code_key UNIQUE (error_code),
                                            CONSTRAINT meta_error_registry_lookup_workflow_name_key UNIQUE (lookup, workflow_name),
                                            CONSTRAINT meta_error_registry_pkey PRIMARY KEY (id)
);

CREATE TABLE public.meta_event_handler (
                                           id bigserial NOT NULL,
                                           created_on timestamp NOT NULL DEFAULT now(),
                                           modified_on timestamp NOT NULL DEFAULT now(),
                                           "name" varchar(255) NOT NULL,
                                           "event" varchar(255) NOT NULL,
                                           active bool NOT NULL,
                                           json_data text NOT NULL,
                                           CONSTRAINT meta_event_handler_pkey PRIMARY KEY (id)
);

CREATE TABLE public.meta_priority (
                                      id bigserial NOT NULL,
                                      created_on timestamp NOT NULL DEFAULT now(),
                                      modified_on timestamp NOT NULL DEFAULT now(),
                                      min_priority int4 NOT NULL,
                                      max_priority int4 NOT NULL,
                                      "name" varchar(255) NOT NULL,
                                      value text NOT NULL,
                                      CONSTRAINT meta_priority_pkey PRIMARY KEY (id)
);

CREATE TABLE public.meta_task_def (
                                      id bigserial NOT NULL,
                                      created_on timestamp NOT NULL DEFAULT now(),
                                      modified_on timestamp NOT NULL DEFAULT now(),
                                      "name" varchar(255) NOT NULL,
                                      json_data text NOT NULL,
                                      CONSTRAINT meta_task_def_pkey PRIMARY KEY (id)
);

CREATE TABLE public.meta_workflow_def (
                                          id bigserial NOT NULL,
                                          created_on timestamp NOT NULL DEFAULT now(),
                                          modified_on timestamp NOT NULL DEFAULT now(),
                                          "name" varchar(255) NOT NULL,
                                          "version" int4 NOT NULL,
                                          latest_version int4 NOT NULL DEFAULT 0,
                                          json_data text NOT NULL,
                                          CONSTRAINT meta_workflow_def_pkey PRIMARY KEY (id)
);

CREATE TABLE public.poll_data (
                                  id bigserial NOT NULL,
                                  created_on timestamp NOT NULL DEFAULT now(),
                                  modified_on timestamp NOT NULL DEFAULT now(),
                                  queue_name varchar(255) NOT NULL,
                                  "domain" varchar(255) NOT NULL,
                                  json_data text NOT NULL,
                                  CONSTRAINT poll_data_fields UNIQUE (queue_name, domain),
                                  CONSTRAINT poll_data_pkey PRIMARY KEY (id)
);

CREATE TABLE public.queue (
                              id bigserial NOT NULL,
                              created_on timestamp NOT NULL DEFAULT now(),
                              queue_name varchar(255) NOT NULL,
                              CONSTRAINT queue_name UNIQUE (queue_name),
                              CONSTRAINT queue_pkey PRIMARY KEY (id)
);

CREATE TABLE public.queue_message (
                                      id bigserial NOT NULL,
                                      queue_name varchar(255) NOT NULL,
                                      message_id varchar(255) NOT NULL,
                                      "version" int8 NOT NULL DEFAULT 0,
                                      popped bool NOT NULL DEFAULT false,
                                      unacked bool NOT NULL DEFAULT false,
                                      deliver_on timestamp NULL,
                                      unack_on timestamp NULL,
                                      payload text NULL,
                                      priority int4 NOT NULL DEFAULT 0,
                                      CONSTRAINT queue_message_pkey PRIMARY KEY (id),
                                      CONSTRAINT queue_name_msg UNIQUE (queue_name, message_id)
);
CREATE INDEX queue_message_deliver_on ON public.queue_message USING btree (deliver_on);
CREATE INDEX queue_message_message_id ON public.queue_message USING btree (message_id);
CREATE INDEX queue_message_unack_on ON public.queue_message USING btree (unack_on);

CREATE TABLE public.task (
                             id bigserial NOT NULL,
                             created_on timestamp NOT NULL DEFAULT now(),
                             modified_on timestamp NOT NULL DEFAULT now(),
                             task_id varchar(255) NOT NULL,
                             task_type varchar(255) NOT NULL,
                             task_refname varchar(255) NOT NULL,
                             task_status varchar(255) NOT NULL,
                             workflow_id varchar(255) NOT NULL,
                             json_data text NOT NULL,
                             "input" text NULL,
                             "output" text NULL,
                             start_time timestamp NULL,
                             end_time timestamp NULL,
                             CONSTRAINT task_pkey PRIMARY KEY (id),
                             CONSTRAINT task_task_id UNIQUE (task_id)
);
CREATE INDEX task_type_start ON public.task USING btree (task_type, start_time);
CREATE INDEX task_type_status ON public.task USING btree (task_type, task_status);
CREATE INDEX task_workflow_id ON public.task USING btree (workflow_id);

CREATE TABLE public.task_in_progress (
                                         id bigserial NOT NULL,
                                         created_on timestamp NOT NULL DEFAULT now(),
                                         modified_on timestamp NOT NULL DEFAULT now(),
                                         in_progress bool NOT NULL DEFAULT false,
                                         task_def_name varchar(255) NOT NULL,
                                         task_id varchar(255) NOT NULL,
                                         workflow_id varchar(255) NOT NULL,
                                         CONSTRAINT task_in_progress_fields UNIQUE (task_def_name, task_id),
                                         CONSTRAINT task_in_progress_pkey PRIMARY KEY (id)
);
CREATE INDEX task_in_progress_wfid ON public.task_in_progress USING btree (workflow_id);


CREATE TABLE public.task_rate_limit (
                                        id bigserial NOT NULL,
                                        created_on timestamp NOT NULL DEFAULT now(),
                                        expires_on timestamp NOT NULL,
                                        task_def_name varchar(255) NOT NULL,
                                        CONSTRAINT task_rate_limit_pkey PRIMARY KEY (id)
);
CREATE INDEX task_rate_limit_name_created ON public.task_rate_limit USING btree (task_def_name, created_on);


CREATE TABLE public.task_scheduled (
                                       id bigserial NOT NULL,
                                       created_on timestamp NOT NULL DEFAULT now(),
                                       modified_on timestamp NOT NULL DEFAULT now(),
                                       workflow_id varchar(255) NOT NULL,
                                       task_key varchar(255) NOT NULL,
                                       task_id varchar(255) NOT NULL,
                                       CONSTRAINT task_scheduled_pkey PRIMARY KEY (id),
                                       CONSTRAINT task_scheduled_wf_task UNIQUE (workflow_id, task_key)
);


CREATE TABLE public.workflow (
                                 id bigserial NOT NULL,
                                 created_on timestamp NOT NULL DEFAULT now(),
                                 modified_on timestamp NOT NULL DEFAULT now(),
                                 start_time timestamp NULL,
                                 end_time timestamp NULL,
                                 parent_workflow_id varchar(255) NULL,
                                 workflow_id varchar(255) NOT NULL,
                                 workflow_type varchar(255) NOT NULL,
                                 workflow_status varchar(255) NOT NULL,
                                 date_str int4 NOT NULL,
                                 json_data text NOT NULL,
                                 "input" text NULL,
                                 "output" text NULL,
                                 correlation_id text NULL,
                                 tags _text NULL,
                                 CONSTRAINT workflow_pkey PRIMARY KEY (id),
                                 CONSTRAINT workflow_workflow_id UNIQUE (workflow_id)
);
CREATE INDEX workflow_end_time ON public.workflow USING btree (end_time);
CREATE INDEX workflow_parent_workflow_id ON public.workflow USING btree (parent_workflow_id);
CREATE INDEX workflow_start_time ON public.workflow USING btree (start_time);
CREATE INDEX workflow_tags ON public.workflow USING gin (tags);
CREATE INDEX workflow_type_status_date ON public.workflow USING btree (workflow_type, workflow_status, date_str);
CREATE INDEX workflow_workflow_status_idx ON public.workflow USING btree (workflow_status);


CREATE TABLE public.workflow_error_registry (
                                                id bigserial NOT NULL,
                                                created_on timestamp NOT NULL DEFAULT now(),
                                                start_time timestamp NULL,
                                                end_time timestamp NULL,
                                                parent_workflow_id varchar(255) NULL,
                                                workflow_id varchar(255) NOT NULL,
                                                workflow_type varchar(255) NOT NULL,
                                                workflow_status varchar(255) NOT NULL,
                                                complete_error text NULL,
                                                job_id varchar(255) NULL,
                                                ranking_id varchar(255) NULL,
                                                order_id varchar(255) NULL,
                                                error_lookup_id int4 NULL,
                                                CONSTRAINT workflow_error_registry_pkey PRIMARY KEY (id)
);

CREATE TABLE public.task_log (
                                 id bigserial NOT NULL,
                                 created_on timestamp NOT NULL DEFAULT now(),
                                 task_id varchar(255) NOT NULL,
                                 log text NOT NULL,
                                 CONSTRAINT task_log_pkey PRIMARY KEY (id),
                                 CONSTRAINT task_log_task_id_fkey FOREIGN KEY (task_id) REFERENCES public.task(task_id) ON DELETE CASCADE
);
CREATE INDEX task_log_task_id ON public.task_log USING btree (task_id);

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