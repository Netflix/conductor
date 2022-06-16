ALTER TABLE workflow ADD json_data_workflow_ids jsonb;

UPDATE workflow SET json_data_workflow_ids = json_data::jsonb->'workflowIds';

CREATE INDEX workflow_json_data_workflow_ids_gin_idx ON workflow
    USING gin (json_data_workflow_ids jsonb_path_ops);

CREATE OR REPLACE FUNCTION copy_workflow_ids() RETURNS TRIGGER AS $$
BEGIN
  NEW.json_data_workflow_ids := NEW.json_data::jsonb->'workflowIds';
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER copy_workflow_ids_trigger
    BEFORE INSERT OR UPDATE OF json_data ON workflow
    FOR EACH ROW EXECUTE PROCEDURE copy_workflow_ids();