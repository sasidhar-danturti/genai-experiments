"""Lambda handlers that expose the document processing API surface."""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:  # pragma: no cover - boto3 is not installed in unit test environments.
    import boto3
    from botocore.exceptions import ClientError
except Exception:  # pragma: no cover
    boto3 = None
    ClientError = Exception

from .contracts import (
    ISO8601_FORMAT,
    CompletionNotificationPayload,
    JobResultsResponse,
    JobStatus,
    SubmitJobRequest,
    SubmitJobResponse,
    parse_job_status_record,
)
from .databricks_sql_client import DatabricksSQLClient
from .notifications import dispatch_completion_notifications

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def submit_job_handler(event: Dict[str, Any], context: Optional[Any]) -> Dict[str, Any]:
    """Accept a new document processing request and enqueue it for ingestion."""

    if boto3 is None:  # pragma: no cover
        raise RuntimeError("boto3 is required to submit jobs")

    request = SubmitJobRequest.from_api_gateway_event(event)
    job_id = str(uuid.uuid4())
    queue_url = os.environ["INGESTION_QUEUE_URL"]
    sqs_client = boto3.client("sqs")

    message_payload = request.to_message_payload(job_id)
    try:
        sqs_response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message_payload))
    except ClientError as exc:  # pragma: no cover - requires AWS
        LOGGER.exception("Failed to publish message to SQS")
        return _error_response(502, f"Failed to enqueue job: {exc}")

    now = datetime.now(timezone.utc).strftime(ISO8601_FORMAT)
    _persist_initial_job_state(job_id, request, now)

    estimated_latency_ms = int(os.environ.get("QUEUE_SLA_MS", "120000"))
    response = SubmitJobResponse(
        job_id=job_id,
        status=JobStatus.QUEUED,
        queue_message_id=sqs_response.get("MessageId", "unknown"),
        estimated_latency_ms=estimated_latency_ms,
    )
    return _json_response(202, response.to_dict())


def get_job_status_handler(event: Dict[str, Any], context: Optional[Any]) -> Dict[str, Any]:
    """Return the lifecycle metadata for a submitted job."""

    job_id = _extract_job_id(event)
    if not job_id:
        return _error_response(400, "job_id path parameter is required")

    record = _load_job_record(job_id)
    if record is None:
        return _error_response(404, f"Job {job_id} not found")

    payload = parse_job_status_record(record)
    return _json_response(200, payload.to_dict())


def fetch_results_handler(event: Dict[str, Any], context: Optional[Any]) -> Dict[str, Any]:
    """Return canonical results for a completed job."""

    job_id = _extract_job_id(event)
    if not job_id:
        return _error_response(400, "job_id path parameter is required")

    job_record = _load_job_record(job_id)
    if job_record is None:
        return _error_response(404, f"Job {job_id} not found")

    status = JobStatus(job_record["status"])
    if status not in {JobStatus.SUCCEEDED, JobStatus.PARTIALLY_SUCCEEDED}:
        return _error_response(409, f"Job {job_id} is not ready. Current status: {status.value}")

    page_size = int(os.environ.get("RESULTS_PAGE_SIZE", "50"))
    page_size_param = _extract_query_param(event, "page_size")
    if page_size_param:
        try:
            requested_page_size = int(page_size_param)
            if requested_page_size > 0:
                page_size = min(requested_page_size, page_size)
        except ValueError:
            return _error_response(400, "page_size must be a positive integer")
    page_token = _extract_query_param(event, "page_token")

    client = _create_databricks_client()
    documents, next_page_token = client.fetch_canonical_documents(job_id, page_size=page_size, page_token=page_token)

    response = JobResultsResponse(
        job_id=job_id,
        status=status,
        documents=documents,
        next_page_token=next_page_token,
    )

    if _should_emit_notifications(job_record):
        payload = CompletionNotificationPayload(
            job_id=job_id,
            status=status,
            documents=documents,
            enrichments=job_record.get("enrichments", []),
            published_at=datetime.now(timezone.utc),
        )
        try:
            dispatch_completion_notifications(job_record, payload)
        except Exception:  # pragma: no cover - network/SNS interactions
            LOGGER.exception("Failed to dispatch notifications for job %s", job_id)
        else:
            _mark_notifications_emitted(job_id)

    return _json_response(200, response.to_dict())


def _should_emit_notifications(job_record: Dict[str, Any]) -> bool:
    config = job_record.get("notification_config")
    if not config:
        return False
    if job_record.get("notifications_emitted"):
        return False
    return True


def _extract_job_id(event: Dict[str, Any]) -> Optional[str]:
    path_params = event.get("pathParameters") or {}
    return path_params.get("job_id")


def _extract_query_param(event: Dict[str, Any], name: str) -> Optional[str]:
    params = event.get("queryStringParameters") or {}
    return params.get(name)


def _json_response(status_code: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload),
    }


def _error_response(status_code: int, message: str) -> Dict[str, Any]:
    LOGGER.warning("API request failed: %s", message)
    return _json_response(status_code, {"error": message})


def _persist_initial_job_state(job_id: str, request: SubmitJobRequest, timestamp: str) -> None:
    if boto3 is None:  # pragma: no cover
        raise RuntimeError("boto3 is required to persist job state")

    table_name = os.environ["JOB_STATUS_TABLE_NAME"]
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    item: Dict[str, Any] = {
        "job_id": job_id,
        "status": JobStatus.QUEUED.value,
        "submitted_at": timestamp,
        "updated_at": timestamp,
        "metadata": request.metadata,
    }
    if request.notification_config:
        item["notification_config"] = request.notification_config.to_dict()
    table.put_item(Item=item)


def _load_job_record(job_id: str) -> Optional[Dict[str, Any]]:
    if boto3 is None:  # pragma: no cover
        raise RuntimeError("boto3 is required to load job state")

    table_name = os.environ["JOB_STATUS_TABLE_NAME"]
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    result = table.get_item(Key={"job_id": job_id})
    return result.get("Item") if result else None


def _create_databricks_client() -> DatabricksSQLClient:
    return DatabricksSQLClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
        endpoint_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        catalog=os.environ.get("RESULTS_CATALOG", "lakehouse"),
        schema=os.environ.get("RESULTS_SCHEMA", "document_intelligence"),
        table=os.environ.get("RESULTS_TABLE", "enriched_documents"),
    )


def _mark_notifications_emitted(job_id: str) -> None:
    if boto3 is None:  # pragma: no cover
        raise RuntimeError("boto3 is required to update job state")

    table_name = os.environ["JOB_STATUS_TABLE_NAME"]
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    timestamp = datetime.now(timezone.utc).strftime(ISO8601_FORMAT)
    table.update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET notifications_emitted = :true, updated_at = :updated_at",
        ExpressionAttributeValues={":true": True, ":updated_at": timestamp},
    )
