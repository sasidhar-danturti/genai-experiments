# Databricks Ingestion Runbook

## Overview

The Databricks ingestion workflow orchestrates SQS batch processing, document
routing, and downstream parsing. This runbook outlines the operational
procedures for monitoring the pipeline, responding to alerts, replaying
dead-letter messages, and applying emergency overrides.

## Structured Telemetry

### Delta + MLflow events

* Job lifecycle events are written to the Delta table configured via the
  `OBSERVABILITY_TABLE` environment variable (default:
  `lakehouse.raw_ingestion_events`). Each record captures job status, queue
  depth snapshots, batch throughput, and failure context.
* When MLflow is available, the job attaches tags and metrics to the active run
  so that processing duration, failure counts, and contextual metadata are
  visible within the Databricks experiment UI.

### CloudWatch metrics

* Custom metrics are emitted under the namespace configured by
  `CLOUDWATCH_NAMESPACE` (default: `DocumentProcessing`). Metrics include:
  * `ParserSuccess` and `ParserFailure` counters for success-rate analysis.
  * `ParserLatencyMs` to capture end-to-end routing latency.
  * `QueueDepthVisible`/`QueueDepthInFlight` snapshots derived from SQS.
* Metrics feed the managed dashboard (`${var.project}-ingestion-observability`)
  and CloudWatch alarms defined in Terraform:
  * `queue-depth-high` alerts when visible messages exceed the configured
    threshold (default 100).
  * `parser-failure-rate` alerts when the proportion of failures breaches the
    acceptable ceiling (default 20%).

## Alert Response Playbooks

### Queue depth alarm

1. Check the CloudWatch dashboard for the affected queue to confirm sustained
   backlog growth.
2. Inspect the `raw_ingestion_events` Delta table for recent `batch_processed`
   events and verify that batches are completing successfully.
3. Scale Databricks worker capacity or increase the Jobs concurrency if the
   backlog is caused by throughput limits.

### Parser failure rate alarm

1. Review the `raw_ingestion_failures` Delta table for the relevant time window
   to determine dominant failure reasons (`invalid_json`,
   `missing_object_key`, `routing_error`, etc.).
2. Inspect MLflow run details for the job attempt to correlate failures with
   configuration changes or upstream data anomalies.
3. If failures stem from known bad payloads, consider pausing upstream
   publishers or applying routing overrides.

## Dead-Letter Queue (DLQ) Management

### Automatic redrive behaviour

* Processing errors leave the message visible until the SQS redrive policy moves
  it to the `${stream}-dlq` queue after five unsuccessful attempts (configurable
  via Terraform).
* Each failure is logged to the `raw_ingestion_failures` Delta table with the
  SQS receive count and stack trace for triage.

### Manual replay

Use the Databricks DLQ replay utility when the underlying issue has been
resolved:

```bash
python -m databricks.dlq_replay \
  https://sqs.<region>.amazonaws.com/<account>/<stream>-dlq \
  https://sqs.<region>.amazonaws.com/<account>/<stream> \
  --region <region> \
  --limit 100 \
  --throttle 0.5
```

* Add `--dry-run` to list DLQ messages without deleting them.
* Adjust `--limit` and `--throttle` to control the replay rate and avoid
  overwhelming downstream parsers.

## Emergency Overrides

* **Routing overrides:** Update the Delta override table or secrets-backed JSON
  payload and allow the job to reload overrides on the next polling interval.
* **Pause ingestion:** Disable the Databricks job schedule or detach the SQS
  subscription in AWS for the affected stream to stop new batches.
* **Force retry with backfill:** After resolving the incident, use the replay
  utility above or republish raw objects to the SNS topic to trigger standard
  ingestion.

## Escalation

1. On-call data engineer (primary) – monitor CloudWatch alarms and Databricks
   job state.
2. Platform reliability engineer (secondary) – engage when alarms persist for
   more than two polling intervals or when DLQ replay exceeds configured limits.

Document all incidents, replay counts, and overrides in the shared operations
channel for post-incident review.
