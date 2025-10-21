"""Databricks notebook entry point for processing batched SQS payloads.

This notebook is designed to run as a Databricks Job with multi-task workflows.
It supports configurable batch sizes, fan-out to worker clusters via the Jobs
API, and persists raw metadata to Delta Lake for monitoring/replay.
"""

import concurrent.futures
import json
import os
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional

import boto3
from botocore.config import Config

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit

# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IngestionConfig:
    """Runtime configuration for the ingestion job."""

    queue_url: str
    region: str
    max_batch_size: int = 50
    visibility_timeout_buffer: int = 30
    wait_time_seconds: int = 20
    poll_interval_seconds: int = 5
    max_batches: Optional[int] = None
    dispatch_job_id: Optional[str] = None
    worker_task_parameters: Optional[dict] = None
    metadata_table: str = "lakehouse.raw_ingestion_metadata"


# ---------------------------------------------------------------------------
# SQS integration
# ---------------------------------------------------------------------------


def _create_sqs_client(region: str):
    return boto3.client(
        "sqs",
        region_name=region,
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )


def receive_message_batch(sqs_client, queue_url: str, config: IngestionConfig):
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=min(config.max_batch_size, 10),
        WaitTimeSeconds=config.wait_time_seconds,
        VisibilityTimeout=config.visibility_timeout_buffer,
        MessageAttributeNames=["All"],
    )
    return response.get("Messages", [])


def delete_messages(sqs_client, queue_url: str, receipt_handles: Iterable[str]):
    if not receipt_handles:
        return

    sqs_client.delete_message_batch(
        QueueUrl=queue_url,
        Entries=[{"Id": str(idx), "ReceiptHandle": handle} for idx, handle in enumerate(receipt_handles)],
    )


# ---------------------------------------------------------------------------
# Metadata persistence
# ---------------------------------------------------------------------------


def persist_metadata(spark: SparkSession, metadata_table: str, records: List[dict]):
    if not records:
        return

    df: DataFrame = spark.createDataFrame(records)

    (df.withColumn("ingested_at", current_timestamp())
       .write
       .format("delta")
       .mode("append")
       .saveAsTable(metadata_table))


# ---------------------------------------------------------------------------
# Workload dispatch
# ---------------------------------------------------------------------------


def dispatch_to_worker(job_id: str, payload: List[dict], task_parameters: Optional[dict] = None):
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    run = client.jobs.submit(
        job_id=job_id,
        run_name=f"ingestion-worker-{int(time.time())}",
        tasks=[
            {
                "task_key": "worker",
                "existing_cluster_id": task_parameters.get("existing_cluster_id") if task_parameters else None,
                "new_cluster": task_parameters.get("new_cluster") if task_parameters else None,
                "notebook_task": {
                    "notebook_path": task_parameters["notebook_path"],
                    "base_parameters": {
                        "payload": json.dumps(payload),
                    },
                },
            }
        ],
    )
    return run


# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------


def process_messages(config: IngestionConfig):
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    sqs_client = _create_sqs_client(config.region)

    batches_processed = 0
    total_messages = 0

    while True:
        messages = receive_message_batch(sqs_client, config.queue_url, config)
        if not messages:
            time.sleep(config.poll_interval_seconds)
            continue

        receipt_handles = [message["ReceiptHandle"] for message in messages]
        payload = [json.loads(message["Body"]) for message in messages]

        metadata_records = [
            {
                "source_path": body.get("s3", {}).get("object", {}).get("key"),
                "file_type": os.path.splitext(body.get("s3", {}).get("object", {}).get("key", ""))[1].lstrip("."),
                "message_id": message["MessageId"],
                "sns_topic": body.get("TopicArn"),
                "queue_url": config.queue_url,
            }
            for message, body in zip(messages, payload)
        ]

        persist_metadata(spark, config.metadata_table, metadata_records)

        if config.dispatch_job_id:
            dispatch_to_worker(
                config.dispatch_job_id,
                payload,
                task_parameters=config.worker_task_parameters,
            )
        else:
            # Inline processing placeholder: replace with domain-specific logic.
            with concurrent.futures.ThreadPoolExecutor() as executor:
                list(executor.map(lambda body: body, payload))

        delete_messages(sqs_client, config.queue_url, receipt_handles)

        batches_processed += 1
        total_messages += len(messages)

        if config.max_batches and batches_processed >= config.max_batches:
            break

    spark.createDataFrame([(total_messages, batches_processed)], ["messages", "batches"]).withColumn(
        "queue_url", lit(config.queue_url)
    ).withColumn(
        "completed_at", current_timestamp()
    ).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        f"{config.metadata_table}_run_summary"
    )


if __name__ == "__main__":
    config = IngestionConfig(
        queue_url=os.environ["INGESTION_QUEUE_URL"],
        region=os.environ.get("AWS_REGION", "us-east-1"),
        max_batch_size=int(os.environ.get("MAX_BATCH_SIZE", "50")),
        visibility_timeout_buffer=int(os.environ.get("VISIBILITY_TIMEOUT_BUFFER", "120")),
        wait_time_seconds=int(os.environ.get("WAIT_TIME_SECONDS", "20")),
        poll_interval_seconds=int(os.environ.get("POLL_INTERVAL_SECONDS", "5")),
        max_batches=int(os.environ.get("MAX_BATCHES", "0")) or None,
        dispatch_job_id=os.environ.get("DISPATCH_JOB_ID"),
        worker_task_parameters=json.loads(os.environ.get("WORKER_TASK_PARAMETERS", "{}")),
        metadata_table=os.environ.get("METADATA_TABLE", "lakehouse.raw_ingestion_metadata"),
    )

    process_messages(config)
