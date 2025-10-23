# Document Processing API Runbook

## Overview

The Document Processing API exposes a fully managed interface for orchestrating
asynchronous document intelligence workloads. Requests are accepted by API
Gateway, persisted to SQS for downstream ingestion, and tracked in a DynamoDB
status ledger. Canonicalised results are materialised into the
`{catalog}.{schema}.{table}` Delta table that powers analytics and
integrations.

```
Client -> API Gateway -> Lambda (submit_job_handler)
                         |-- SQS (ingestion queue)
                         |-- DynamoDB (job status)
                         |-- Databricks SQL endpoint (results)
                         '-- SNS/Webhook notifications
```

All responses conform to the canonical schema defined in
`parsers/canonical_schema.py`, ensuring parity with downstream enrichment and
storage expectations.

## Endpoint catalogue

### `POST /jobs`

Submit a new document for processing. The payload controls routing metadata,
notification behaviour, and optional enrichment hints.

```json
{
  "source_uri": "s3://bucket/contracts/2024-09-14/acme.pdf",
  "checksum": "2c26b46b68ffc68ff99b453c1d30413413422",
  "document_type": "contract",
  "mime_type": "application/pdf",
  "priority": "high",
  "metadata": {
    "tenant_id": "acme",
    "requested_enrichments": ["summaries", "tables"]
  },
  "notification_config": {
    "sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:doc-processing-events",
    "webhook_url": "https://example.net/hooks/doc-finished",
    "include_enrichment_events": true
  }
}
```

*Response – `202 Accepted`*

```json
{
  "job_id": "a71f5e25-90be-4a73-a099-ef6d4de2e11d",
  "status": "queued",
  "queue_message_id": "3b2d7650-6720-5c64-86bb-91ad94d07d03",
  "estimated_latency_ms": 120000
}
```

### `GET /jobs/{job_id}`

Return lifecycle metadata and enrichment progress for a job. Enrichment entries
adhere to the enumerations exposed by `EnrichmentStatus`.

```json
{
  "job_id": "a71f5e25-90be-4a73-a099-ef6d4de2e11d",
  "status": "running",
  "submitted_at": "2024-05-19T12:10:13.284Z",
  "updated_at": "2024-05-19T12:14:28.601Z",
  "enrichments": [
    {
      "name": "summaries",
      "status": "succeeded",
      "started_at": "2024-05-19T12:10:45.102Z",
      "completed_at": "2024-05-19T12:12:02.440Z"
    },
    {
      "name": "tables",
      "status": "running",
      "started_at": "2024-05-19T12:12:08.190Z"
    }
  ]
}
```

### `GET /jobs/{job_id}/results`

Retrieve canonical documents that match the schema defined by
`CanonicalDocument`. Results are paginated via `page_token` and surfaced
straight from the Databricks SQL endpoint backing the enrichment table.

```json
{
  "job_id": "a71f5e25-90be-4a73-a099-ef6d4de2e11d",
  "status": "succeeded",
  "documents": [
    {
      "document_id": "acme-2024-09-14",
      "source_uri": "s3://bucket/contracts/2024-09-14/acme.pdf",
      "checksum": "2c26b46b68ffc68ff99b453c1d30413413422",
      "schema_version": "1.1",
      "metadata": {
        "tenant_id": "acme",
        "parsers_used": ["pymupdf", "databricks_llm_image"]
      },
      "text_spans": [
        {
          "content": "Agreement between ACME and Example Co.",
          "confidence": 0.99,
          "span_id": "span-0",
          "provenance": {
            "parser": "pymupdf",
            "model": "pymupdf-1.23.5"
          }
        }
      ],
      "tables": [],
      "fields": [],
      "summaries": [
        {
          "summary_type": "executive",
          "content": "Contract renews the ACME–Example agreement for 12 months.",
          "confidence": 0.92,
          "provider": "databricks_llm"
        }
      ],
      "enrichments": [
        {
          "name": "entity_extraction",
          "status": "succeeded",
          "provider": "databricks_llm",
          "content": {
            "parties": ["ACME", "Example Co."],
            "term_months": 12
          }
        }
      ]
    }
  ]
}
```

When the results table contains additional records for the same job, the API
returns a `next_page_token` that can be supplied in subsequent calls.

## Notification hooks

Completion events are pushed to the configured SNS topic and/or webhook with the
following payload. The `enrichments` array is omitted when
`include_enrichment_events` is set to `false`.

```json
{
  "job_id": "a71f5e25-90be-4a73-a099-ef6d4de2e11d",
  "status": "succeeded",
  "documents": [... canonical payloads ...],
  "enrichments": [
    {
      "name": "summaries",
      "status": "succeeded",
      "detail": "Generated via databricks_llm"
    }
  ],
  "published_at": "2024-05-19T12:16:31.121Z"
}
```

Webhook destinations must respond with a 2xx status code within ten seconds to
avoid retries. SNS publishes use the default exponential backoff retry policy.

## Environment configuration

| Variable | Purpose |
| --- | --- |
| `INGESTION_QUEUE_URL` | Target SQS queue for ingestion workers. |
| `JOB_STATUS_TABLE_NAME` | DynamoDB table that tracks job lifecycle metadata. |
| `QUEUE_SLA_MS` | Optional SLA hint returned to clients (default `120000`). |
| `DATABRICKS_HOST` | Workspace hostname used for Databricks SQL REST calls. |
| `DATABRICKS_TOKEN` | PAT with access to the configured warehouse. |
| `DATABRICKS_WAREHOUSE_ID` | SQL warehouse that executes result queries. |
| `RESULTS_CATALOG`/`RESULTS_SCHEMA`/`RESULTS_TABLE` | Fully qualified Delta location for canonical documents. |
| `RESULTS_PAGE_SIZE` | Maximum number of canonical documents returned per page (default `50`). |

## Operational guidance

* Lambda functions surface structured logging for each request, enabling query
  of job IDs across CloudWatch insights.
* DynamoDB TTL can be applied to archival attributes if long-term history is
  stored in Delta.
* Databricks SQL queries use statement parameters to protect against SQL
  injection and to reuse the statement cache.
* The notification dispatcher marks records with `notifications_emitted=true`
  to ensure idempotent behaviour across retries.

## Sample IAM policy

```hcl
resource "aws_iam_role" "api_lambda_role" {
  name = "document-processing-api"

  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "api_lambda_policy" {
  name = "document-processing-api"
  role = aws_iam_role.api_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = ["sqs:SendMessage"],
        Resource = aws_sqs_queue.ingestion.arn
      },
      {
        Effect = "Allow",
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem"
        ],
        Resource = aws_dynamodb_table.job_status.arn
      },
      {
        Effect = "Allow",
        Action = ["sns:Publish"],
        Resource = "arn:aws:sns:us-east-1:123456789012:doc-processing-events"
      }
    ]
  })
}
```

## Troubleshooting

1. **Job remains queued** – Inspect SQS metrics for backlog and confirm
   downstream workers are healthy. Consider scaling ingestion capacity or
   reprocessing dead-letter messages.
2. **Results endpoint returns `409`** – The job has not yet reached a terminal
   state. Use `GET /jobs/{job_id}` to view enrichment progress and confirm
   downstream completion.
3. **Notifications missing** – Verify that the SNS topic and webhook endpoints
   are reachable. CloudWatch logs capture any publish failures emitted by the
   Lambda runtime.
