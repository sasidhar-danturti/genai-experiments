## Cloud ingestion experiments

This repository now contains an end-to-end reference for driving file-based
ingestion events from cloud object storage through AWS messaging services and
into Databricks for downstream processing.

### Components

1. **Infrastructure-as-code** – `infrastructure/aws/cloud_storage_ingestion.tf`
   provisions an S3 bucket that emits object-created notifications to an SNS
   topic. The topic fans out to one or more SQS queues (each with an attached
   DLQ) so individual workloads can process distinct streams. A CloudFormation
   alternative is available at `infrastructure/aws/cloud_storage_ingestion.yaml`
   if you prefer YAML-based stacks over Terraform.
2. **Databricks polling job** – `idp_service/sqs_batch_ingestion.py` is a Python
   notebook/script that can be scheduled as a Databricks Job. It performs batch
   pulls from SQS, optionally dispatches work units to dedicated worker
   clusters via the Jobs API, and records every message in Delta tables for
   monitoring and replay.
3. **Delta Lake tracking tables** – `delta/raw_ingestion_metadata.sql` creates
   the metadata and run-summary tables that persist ingestion state.
4. **Document Processing API** – `services/document_processing_api` provides
   Lambda handlers and supporting utilities that expose submit/status/result
   endpoints via API Gateway. Requests enqueue work to SQS, statuses are backed
   by DynamoDB, and results stream from Databricks SQL, all while returning the
   canonical schema described in `parsers/canonical_schema.py`.

   For a walkthrough of the IDP pipeline design and how the pieces fit
   together, see [`docs/idp_service_architecture.md`](docs/idp_service_architecture.md).

### Usage notes

- Customize Terraform variables (`bucket_name`, `sqs_subscribers`, etc.) to fit
  your environment and apply with `terraform init && terraform apply`.
- Import the Databricks notebook or run it as a Python wheel/DBX task with the
  necessary environment variables (e.g., `INGESTION_QUEUE_URL`).
- Execute the SQL script in a Databricks SQL warehouse or notebook to create
  the tracking tables before running the job.
- Review `docs/runbooks/document_processing_api.md` for the API contract, deployment runbook, and IAM references.
- Run `notebooks/idp_production_simulation.ipynb` to rehearse the end-to-end flow with production-style configuration TODOs and simulated SQS batches before wiring up live services.
- Run `notebooks/idp_validation_suite.ipynb` to validate routing overrides, enrichment hooks, and idempotent persistence across sample documents before deploying changes.
- Explore the standalone router package via `docs/idp_router/README.md` and the accompanying `notebooks/idp_router_quickstart.ipynb` quickstart to integrate the routing logic into other services.
- Integrate against the API using the helper in `clients/document_processing_client.py`, which demonstrates submission, polling, and result pagination.
