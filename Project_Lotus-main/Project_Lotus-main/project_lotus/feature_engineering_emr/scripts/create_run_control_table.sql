-- One-time: create the feature engineering run control table (same shape as
-- gold_run_control, plus model_type and limit_applied). Run it once in Athena or
-- spark-sql against the Glue catalog, with a role that may create tables.
-- Change the database/table to match run_control.table in conf/*.yaml.

CREATE TABLE IF NOT EXISTS glue_catalog.lotus_sandbox_gold_curated.feature_engineering_run_control (
    job_name        string,
    run_id          string,
    watermark_from  timestamp,
    watermark_to    timestamp,
    slice_size      bigint,
    status          string,     -- running | succeeded | failed
    started_ts      timestamp,
    finished_ts     timestamp,
    model_type      string,     -- training | scoring
    limit_applied   boolean     -- true: a limit or explicit dates were used; the watermark was not advanced
)
USING iceberg;
