-- Delta Lake tables for tracking raw ingestion metadata and run history.

CREATE TABLE IF NOT EXISTS lakehouse.raw_ingestion_metadata (
  source_path STRING,
  file_type STRING,
  message_id STRING,
  sns_topic STRING,
  queue_url STRING,
  ingested_at TIMESTAMP
)
USING delta
PARTITIONED BY (file_type)
LOCATION '/mnt/lakehouse/raw_ingestion_metadata';

CREATE TABLE IF NOT EXISTS lakehouse.raw_ingestion_metadata_run_summary (
  queue_url STRING,
  messages BIGINT,
  batches BIGINT,
  completed_at TIMESTAMP
)
USING delta
LOCATION '/mnt/lakehouse/raw_ingestion_metadata_run_summary';
