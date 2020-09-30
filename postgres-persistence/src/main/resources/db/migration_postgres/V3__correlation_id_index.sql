DROP INDEX IF EXISTS workflow_corr_id_index;

CREATE INDEX workflow_corr_id_index ON conductor.workflow (correlation_id);