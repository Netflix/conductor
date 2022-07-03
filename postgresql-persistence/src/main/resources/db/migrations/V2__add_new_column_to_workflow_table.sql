ALTER TABLE workflow ADD json_data_workflow_ids jsonb;

UPDATE workflow SET json_data_workflow_ids = json_data::jsonb->'workflowIds';

CREATE INDEX workflow_json_data_workflow_ids_gin_idx ON workflow
    USING gin (json_data_workflow_ids jsonb_path_ops);