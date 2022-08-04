CREATE TABLE IF NOT EXISTS log4j_logs
(
    id bigserial NOT NULL,
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
    span_id  text,
    CONSTRAINT log4j_logs_pkey PRIMARY KEY (id)    
);
CREATE INDEX IF NOT EXISTS log4j_logs_log_time_idx on log4j_logs (log_time);

CREATE TABLE IF NOT EXISTS meta_config
(
	name  varchar(255) NOT NULL,
	value text,
	CONSTRAINT meta_config_pkey PRIMARY KEY (name)
);

CREATE TABLE IF NOT EXISTS meta_task_def
(
	id          bigserial    NOT NULL,
	created_on  timestamp    NOT NULL DEFAULT now(),
	modified_on timestamp    NOT NULL DEFAULT now(),
	name        varchar(255) NOT NULL,
	json_data   text         NOT NULL,
	CONSTRAINT meta_task_def_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS meta_workflow_def
(
    id             bigserial    NOT NULL,
    created_on     timestamp    NOT NULL DEFAULT now(),
    modified_on    timestamp    NOT NULL DEFAULT now(),
    name           varchar(255) NOT NULL,
    version        int          NOT NULL,
    latest_version int          NOT NULL DEFAULT 0,
    json_data      text         NOT NULL,
    CONSTRAINT meta_workflow_def_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS meta_event_handler
(
    id          bigserial    NOT NULL,
    created_on  timestamp    NOT NULL DEFAULT now(),
    modified_on timestamp    NOT NULL DEFAULT now(),
    name        varchar(255) NOT NULL,
    event       varchar(255) NOT NULL,
    active      boolean      NOT NULL,
    json_data   text         NOT NULL,
    CONSTRAINT meta_event_handler_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS meta_error_registry
(
    id                    bigserial   NOT NULL,
    error_code            TEXT        NOT NULL,
    lookup                TEXT        NOT NULL,
    workflow_name         TEXT,
    general_message       TEXT,
    root_cause            TEXT,
    resolution            TEXT        NOT NULL,
    isrequiredinreporting boolean     NOT NULL DEFAULT true,
    CONSTRAINT meta_error_registry_error_code_key UNIQUE (error_code),
    CONSTRAINT meta_error_registry_lookup_workflow_name_key UNIQUE (lookup, workflow_name),
    CONSTRAINT meta_error_registry_pkey PRIMARY KEY (id)

);


CREATE TABLE IF NOT EXISTS meta_priority
(
    id           bigserial              NOT NULL,
    created_on   timestamp              NOT NULL DEFAULT now(),
    modified_on  timestamp              NOT NULL DEFAULT now(),
    min_priority INTEGER                NOT NULL,
    max_priority INTEGER                NOT NULL,
    name         VARCHAR(255)           NOT NULL,
    value        TEXT                   NOT NULL,
    CONSTRAINT meta_priority_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS workflow
(
    id bigserial                        NOT NULL,
    created_on timestamp DEFAULT now()  NOT NULL,
    modified_on timestamp DEFAULT now() NOT NULL,
    start_time timestamp,
    end_time timestamp,
    parent_workflow_id varchar(255),
    workflow_id varchar(255) NOT NULL,
    workflow_type varchar(255) NOT NULL,
    workflow_status varchar(255) NOT NULL,
    date_str integer NOT NULL,
    json_data text NOT NULL,
    input text,
    output text,
    correlation_id text,
    tags text[],
    json_data_workflow_ids jsonb NULL,
    CONSTRAINT workflow_pkey PRIMARY KEY (id),
    CONSTRAINT workflow_workflow_id UNIQUE (workflow_id)    
);
CREATE INDEX IF NOT EXISTS workflow_end_time ON workflow (end_time);
CREATE INDEX IF NOT EXISTS workflow_parent_workflow_id ON workflow (parent_workflow_id);
CREATE INDEX IF NOT EXISTS workflow_start_time ON workflow (start_time);
CREATE INDEX IF NOT EXISTS workflow_tags ON workflow USING gin (tags);
CREATE INDEX IF NOT EXISTS workflow_type_status_date ON workflow (workflow_type, workflow_status, date_str);
CREATE INDEX IF NOT EXISTS workflow_workflow_status_idx ON workflow (workflow_status);

CREATE TABLE IF NOT EXISTS task_in_progress
(
    id bigserial                        NOT NULL,
    created_on timestamp DEFAULT now()  NOT NULL,
    modified_on timestamp DEFAULT now() NOT NULL,
    in_progress boolean DEFAULT false   NOT NULL,
    task_def_name varchar(255)          NOT NULL,
    task_id varchar(255)                NOT NULL,
    workflow_id varchar(255)            NOT NULL,
    CONSTRAINT task_in_progress_fields  UNIQUE (task_def_name, task_id),
    CONSTRAINT task_in_progress_pkey    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS task_in_progress_wfid on task_in_progress (workflow_id);

CREATE TABLE IF NOT EXISTS task_rate_limit
(
    id            bigserial    NOT NULL,
    created_on    timestamp    NOT NULL DEFAULT now(),
    expires_on    timestamp    NOT NULL,
    task_def_name varchar(255) NOT NULL,
    CONSTRAINT task_rate_limit_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS task_rate_limit_name_created ON task_rate_limit (task_def_name, created_on);

CREATE TABLE IF NOT EXISTS task
(
    id           bigserial                NOT NULL,
    created_on   timestamp DEFAULT now()  NOT NULL,
    modified_on  timestamp DEFAULT now()  NOT NULL,
    task_id      varchar(255)             NOT NULL,
    task_type    varchar(255)             NOT NULL,
    task_refname varchar(255)             NOT NULL,
    task_status  varchar(255)             NOT NULL,
    workflow_id  varchar(255)             NOT NULL,
    json_data    text                     NOT NULL,
    input        text,
    output       text,
    start_time   timestamp,
    end_time     timestamp,
    CONSTRAINT task_pkey PRIMARY KEY (id),
    CONSTRAINT task_task_id UNIQUE (task_id)
);
CREATE INDEX IF NOT EXISTS task_type_status ON task (task_type, task_status);
CREATE INDEX IF NOT EXISTS task_type_time ON task (task_type, start_time);
CREATE INDEX IF NOT EXISTS task_workflow_id ON task (workflow_id);


CREATE TABLE IF NOT EXISTS task_scheduled
(
    id           bigserial               NOT NULL,
    created_on   timestamp DEFAULT now() NOT NULL,
    modified_on  timestamp DEFAULT now() NOT NULL,
    workflow_id  varchar(255)            NOT NULL,
    task_key     varchar(255)            NOT NULL,
    task_id      varchar(255)            NOT NULL,
    CONSTRAINT task_scheduled_pkey PRIMARY KEY (id),
    CONSTRAINT task_scheduled_wf_task UNIQUE (workflow_id, task_key)
);

CREATE TABLE IF NOT EXISTS task_log
(
    id           bigserial      NOT NULL,
    created_on   timestamp      NOT NULL DEFAULT now(),
    task_id      varchar(255)   NOT NULL,
    log text                    NOT NULL,
    CONSTRAINT task_log_pkey PRIMARY KEY (id),
    CONSTRAINT task_log_task_id_fkey FOREIGN KEY (task_id) REFERENCES task(task_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS task_log_task_id ON task_log (task_id);

CREATE TABLE IF NOT EXISTS poll_data
(
    id          bigserial        NOT NULL,
    created_on  timestamp        NOT NULL DEFAULT now(),
    modified_on timestamp        NOT NULL DEFAULT now(),
    queue_name  varchar(255)     NOT NULL,
    domain      varchar(255)     NOT NULL,
    json_data   text             NOT NULL,
    CONSTRAINT  poll_data_fields UNIQUE (queue_name, domain),
    CONSTRAINT  poll_data_pkey   PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS event_message
(
    id bigserial NOT NULL,
    created_on timestamp    NOT NULL DEFAULT now(),
    queue_name varchar(255) NOT NULL,
    message_id varchar(255) NOT NULL,
    receipt    text,
    json_data  text,
    CONSTRAINT event_message_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS event_message_created_on on event_message (created_on);

CREATE TABLE IF NOT EXISTS event_execution
(
    id           bigserial               NOT NULL,
    created_on   timestamp DEFAULT now() NOT NULL,
    modified_on  timestamp DEFAULT now() NOT NULL,
    handler_name varchar(255)            NOT NULL,
    event_name   varchar(255)            NOT NULL,
    message_id   varchar(255)            NOT NULL,
    execution_id varchar(255)            NOT NULL,
    status       varchar(255)            NOT NULL,
    subject      varchar(255)            NOT NULL,
    received_on  timestamp,
    accepted_on  timestamp,
    started_on   timestamp,
    processed_on timestamp,
    CONSTRAINT event_execution_fields UNIQUE (handler_name, event_name, message_id, execution_id),
    CONSTRAINT event_execution_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS event_execution_combo ON event_execution (subject, received_on);
CREATE INDEX IF NOT EXISTS event_execution_created_on ON event_execution (created_on);

CREATE TABLE IF NOT EXISTS event_published
(
    id bigserial NOT NULL,
    created_on   timestamp    NOT NULL DEFAULT now(),
    json_data    text         NOT NULL,
    message_id   varchar(255) NOT NULL,
    subject      varchar(255) NOT NULL,
    published_on timestamp    NOT NULL,
    CONSTRAINT event_published_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS event_published_created_on ON event_published (created_on);
CREATE INDEX IF NOT EXISTS event_published_subject_date ON event_published (subject, published_on);

CREATE TABLE IF NOT EXISTS queue
(
    id bigserial NOT NULL,
    created_on timestamp DEFAULT now() NOT NULL,
    queue_name varchar(255) NOT NULL,
    CONSTRAINT queue_name UNIQUE (queue_name),
    CONSTRAINT queue_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS queue_message
(
    id bigserial NOT NULL,
    queue_name varchar(255) NOT NULL,
    message_id varchar(255) NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    popped boolean DEFAULT false NOT NULL,
    unacked boolean DEFAULT false NOT NULL,
    deliver_on timestamp,
    unack_on timestamp,
    payload text,
    priority integer DEFAULT 0 NOT NULL,
    CONSTRAINT queue_message_pkey PRIMARY KEY (id),
    CONSTRAINT queue_name_msg UNIQUE (queue_name, message_id)

);
CREATE INDEX IF NOT EXISTS queue_message_deliver_on ON queue_message (deliver_on);
CREATE INDEX IF NOT EXISTS queue_message_message_id ON queue_message (message_id);
CREATE INDEX IF NOT EXISTS queue_message_unack_on ON queue_message (unack_on);

CREATE TABLE IF NOT EXISTS workflow_error_registry (
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

CREATE OR REPLACE FUNCTION get_error_details(p_error_str text, p_workflow_name text)
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

    
---------------------------------------
--------- POPULATE METADATA -----------
---------------------------------------

-------------------
-- UPDATE WORKFLOW FOR EXISTING RECORDS ----
-------------------

UPDATE workflow SET json_data_workflow_ids = json_data::jsonb->'workflowIds';


-------------------
-- META_CONFIG ----
-------------------
    
INSERT INTO META_CONFIG (name,VALUE) VALUES
	 ('srt_to_scc_conversion_process_workflow','deluxe.dependencygraph.srt.scc.conversion.process.1.0'),
	 ('playlist_insert_asset_registration_workflow_name','deluxe.deluxeone.sundog.playlist.insertgroup.registration.process.1.0'),
	 ('srt_to_scc_conversion_workflow','deluxe.ttman.srt.scc.conversion.process.1.0'),
	 ('sundog_distribution_asset_reg_workflow_name','deluxe.deluxeone.sundog.distribution.registration.assetregistration.process.1.0'),
	 ('log4j_logger_com_netflix_conductor_core_execution_batch','DEBUG'),
	 ('ttman_extract_assetreg_workflow_name','deluxe.ttmanextract.assetreg.process.1.0'),
	 ('assetregistration_workflow_name','deluxe.nonepackager.assetReg.process.1.1'),
	 ('contentprep_nasserver','smb://amznfsxhg65mobr.aws.bydeluxe.cloud/share'),
	 ('contentprep_nascopy_tier','Standard'),
	 ('contentprep_nascopy_period','5')
	 on conflict do nothing;
	
INSERT INTO META_CONFIG (name,VALUE) VALUES
	 ('ttmanextract_process','deluxe.ttmanextract.process.1.0'),
	 ('sundog_vfs_restore_workflow_name','deluxe.vfs.restore.process.1.5'),
	 ('WC_LANGUAGE_MAPPING','{"_fra_CAN_":"fr-CA","_fra_FRA_":"fr-FR","_fra_":"fr","_frm_CAN_":"fr-CA","_frm_FRA_":"fr-FR","_frm_":"fr","_eng_US_":"en-US","_eng_UK_":"en-GB","_eng_GBR_":"en-GB","_eng_":"en-US","_enm_US_":"en-US","_enm_UK_":"en-GB","_eng_GBR_":"en-GB","_enm_":"en-US","_spa_LAS_":"es-MX","_spa_SPA_":"es-SP","_spa_ESP_":"es-SP","_spa_":"es"}'),
	 ('assembly_sourcewait_workflow_name','deluxe.dependencygraph.assembly.conformancegroup.source_wait.atlas.process.1.1'),
	 ('log4j_logger_com_netflix_conductor_aurora','INFO'),
	 ('log4j_logger_io_grpc_netty','INFO'),
	 ('log4j_logger_org_eclipse_jetty','INFO'),
	 ('streampackager_drmpackage_workflow_name_am','deluxe.drmpackager.packaging.process.2.1'),
	 ('cc_extract_assetreg_workflow_name','deluxe.ccextract.assetreg.process.1.0'),
	 ('ttman_workflow_name','deluxe.dependencygraph.ttman.process.1.0')
	 on conflict do nothing;
	
INSERT INTO META_CONFIG (name,VALUE) VALUES
	 ('vfs_copy_workflow_name','deluxe.vfs.copy.process.1.2'),
	 ('vfs_workflow_name','deluxe.vfs.getfile.process.1.0'),
	 ('sundog_cpldata_vfsrestore_workflow_name','deluxe.deluxeone.sundog.distribution.cpldata.vfsrestore.process.1.1'),
	 ('playlist_asset_source_restore_workflow_name','deluxe.deluxeone.sundog.playlist.assetrestore.process.1.0'),
	 ('playlist_asset_registration_workflow_name','deluxe.deluxeone.sundog.playlist.registration.process.1.3'),
	 ('audiotracks_asset_registration_workflow_name','deluxe.deluxeone.sundog.audiotrack.registration.1.0'),
	 ('atlasupdate_checksum_workflow_name','deluxe.atlasupdate.checksum.process.1.1'),
	 ('log4j_logger_com_netflix_conductor_contribs_http','INFO'),
	 ('log4j_logger_com_netflix_conductor_server_AccessLogHandler','OFF'),
	 ('ccextract_process','deluxe.ccextract.process.1.0')
	 on conflict do nothing;
	 
INSERT INTO META_CONFIG (name,VALUE) VALUES
	 ('log4j_logger_com_jayway_jsonpath_internal_path_CompiledPath','OFF'),
	 ('log4j_logger_com_netflix_conductor_contribs_validation','INFO'),
	 ('log4j_logger_com_netflix_conductor_core_execution_DeciderService','INFO'),
	 ('log4j_logger_com_netflix_conductor_core_execution_WorkflowExecutor','DEBUG'),
	 ('log4j_logger_com_netflix_conductor_core_execution_WorkflowSweeper','INFO'),
	 ('log4j_logger_org_apache_http','INFO'),
	 ('streampackager_audio_workflow_name_am','deluxe.streampackager.hybrik.transcode.process.2.0'),
	 ('streampackager_drmpackage_workflow_name','deluxe.drmpackager.packaging.process.1.2'),
	 ('streampackager_subtitle_workflow_name','deluxe.streampackager.subtitle.process.1.1'),
	 ('vfs_restore_workflow_name','deluxe.vfs.restore.process.1.5')
	 on conflict do nothing;
	
INSERT INTO META_CONFIG (name,VALUE) VALUES
	 ('WORKFLOW_ACTION_IMAGE_TRANSFORM','deluxe.dependencygraph.action.image_transform.process.1.1'),
	 ('WORKFLOW_ACTION_TRANSCODE_AM','deluxe.dependencygraph.action.transcode.hybrik.process.2.0'),
	 ('WORKFLOW_ACTION_TRANSCODE_DOD_TN','deluxe.dependencygraph.action.transcode.dod_tn.process.1.0'),
	 ('log4j_logger_com_netflix_conductor_contribs_queue_shotgun','DEBUG'),
	 ('log4j_logger_com_netflix_conductor_core_events_shotgun','DEBUG'),
	 ('log4j_logger_com_netflix_conductor_server_resources_WorkflowResource','DEBUG'),
	 ('log4j_logger_com_zaxxer_hikari','INFO'),
	 ('streampackager_package_workflow_name_am','deluxe.streampackager.packager.packaging.process.2.0'),
	 ('vfs_sundog_restore_workflow_name','deluxe.sundog.vfs.restore.process.1.0'),
	 ('WORKFLOW_ACTION_SOURCE_WAIT','deluxe.dependencygraph.action.source_wait.process.1.1')
	 on conflict do nothing;
	
INSERT INTO META_CONFIG (name,VALUE) VALUES
	 ('WORKFLOW_ACTION_TRANSCODE_MEDIACONVERT','deluxe.dependencygraph.action.transcode.mediaconvert.process.1.5'),
	 ('log4j_logger_com_netflix_conductor_core_events_EventProcessor','DEBUG'),
	 ('metadata_process','deluxe.deluxeone.sherlock.packaging.metadata.deliverable.process.1.1'),
	 ('WORKFLOW_ACTION_TIMEDTEXT_TRANSFORM','deluxe.dependencygraph.action.timedtext_transform.process.1.9'),
	 ('contentprep_nascopy_workflow_name','deluxe.deluxeone.contentprep.nascopy.1.0'),
	 ('log4j_logger_com_netflix_conductor_core_execution_tasks_SystemTaskWorkerCoordinator','INFO'),
	 ('WORKFLOW_ACTION_ASSEMBLY_SOURCE_WAIT','deluxe.dependencygraph.assembly.conformancegroup.source_wait.atlas.process.1.1'),
	 ('combined_watermark_transcode_process','deluxe.deluxeone.combinedstreampackager.transcode.process.1.5'),
	 ('log4j_logger_io_swagger','OFF'),
	 ('log4j_logger_org_eclipse_jetty_servlet_DefaultServlet','INFO')
	 on conflict do nothing;
	
INSERT INTO META_CONFIG (name,VALUE) VALUES
	 ('streampackager_drmpackage_assetreg_workflow_name','deluxe.drmpackager.assetReg.process.1.1'),
	 ('WORKFLOW_ACTION_TRANSCODE','deluxe.dependencygraph.action.transcode.hybrik.process.1.13'),
	 ('streampackager_audio_workflow_name','deluxe.streampackager.hybrik.transcode.process.1.5'),
	 ('WORKFLOW_ACTION_TRANSCODE_HYBRIK','deluxe.dependencygraph.action.transcode.hybrik.process.1.13'),
	 ('streampackager_package_workflow_name','deluxe.streampackager.packager.packaging.process.1.2'),
	 ('WORKFLOW_ACTION_TRANSCODE_HYBRIK_AM','deluxe.dependencygraph.action.transcode.hybrik.process.2.0'),
	 ('playlist_asset_groupsource_restore_workflow_name','deluxe.deluxeone.sundog.playlist.audiogroup.assetrestore.process.1.0'),
	 ('combined_watermark_transcode_process_am','deluxe.deluxeone.combinedstreampackager.transcode.process.2.2')
	 on conflict do nothing;
    
    
    
-------------------
-- META_PRIORITY --
-------------------

INSERT INTO META_PRIORITY (CREATED_ON,MODIFIED_ON,MIN_PRIORITY,MAX_PRIORITY,name,VALUE) VALUES
	 ('2020-11-11 11:22:23.990','2020-11-11 11:22:23.990',1,1,'hybrik-transcode-server','transcode-hybrik-lightning'),
	 ('2020-11-11 11:22:24.090','2020-11-11 11:22:24.090',2,2,'hybrik-transcode-server','transcode-hybrik-urgent'),
	 ('2020-11-11 11:22:24.183','2020-11-11 11:22:24.183',3,4,'hybrik-transcode-server','transcode-hybrik-high'),
	 ('2020-11-11 11:22:24.269','2020-11-11 11:22:24.269',5,6,'hybrik-transcode-server','transcode-hybrik-medium'),
	 ('2020-11-11 11:22:24.379','2020-11-11 11:22:24.379',7,8,'hybrik-transcode-server','transcode-hybrik'),
	 ('2020-11-11 11:22:24.487','2020-11-11 11:22:24.487',9,10,'hybrik-transcode-server','transcode-hybrik-low')
	on conflict do nothing;
    

