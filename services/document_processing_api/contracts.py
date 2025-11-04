"""Typed contracts for the document processing API surface."""

from __future__ import annotations

import base64
import json
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from parsers.canonical_schema import SCHEMA_VERSION


ISO8601_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class JobStatus(str, Enum):
    """Lifecycle states for an asynchronous document processing job."""

    ACCEPTED = "accepted"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIALLY_SUCCEEDED = "partially_succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class EnrichmentStatus(str, Enum):
    """Enumerates enrichment lifecycle states."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class NotificationConfig(BaseModel):
    """Destinations that should be notified when processing completes."""

    sns_topic_arn: Optional[str] = None
    webhook_url: Optional[str] = None
    include_enrichment_events: bool = True

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"include_enrichment_events": self.include_enrichment_events}
        if self.sns_topic_arn:
            payload["sns_topic_arn"] = self.sns_topic_arn
        if self.webhook_url:
            payload["webhook_url"] = self.webhook_url
        return payload


class SubmitJobRequest(BaseModel):
    """Incoming payload for ``POST /jobs`` requests."""

    source_uri: str
    checksum: Optional[str] = None
    document_type: Optional[str] = None
    mime_type: Optional[str] = None
    priority: str = "normal"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    notification_config: Optional[NotificationConfig] = None

    @classmethod
    def from_api_gateway_event(cls, event: Dict[str, Any]) -> "SubmitJobRequest":
        """Parse the request body from an API Gateway Lambda proxy event."""

        body = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            body = base64.b64decode(body).decode("utf-8")
        payload = json.loads(body)
        notification = None
        if config := payload.get("notification_config"):
            notification = NotificationConfig(
                sns_topic_arn=config.get("sns_topic_arn"),
                webhook_url=config.get("webhook_url"),
                include_enrichment_events=config.get("include_enrichment_events", True),
            )
        return cls(
            source_uri=payload["source_uri"],
            checksum=payload.get("checksum"),
            document_type=payload.get("document_type"),
            mime_type=payload.get("mime_type"),
            priority=payload.get("priority", "normal"),
            metadata=payload.get("metadata", {}),
            notification_config=notification,
        )

    def to_message_payload(self, job_id: str) -> Dict[str, Any]:
        """Return the SQS message body that downstream workers consume."""

        payload: Dict[str, Any] = {
            "job_id": job_id,
            "source_uri": self.source_uri,
            "priority": self.priority,
            "schema_version": SCHEMA_VERSION,
        }
        if self.checksum:
            payload["checksum"] = self.checksum
        if self.document_type:
            payload["document_type"] = self.document_type
        if self.mime_type:
            payload["mime_type"] = self.mime_type
        if self.metadata:
            payload["metadata"] = self.metadata
        if self.notification_config:
            payload["notification_config"] = self.notification_config.to_dict()
        return payload


class SubmitJobResponse(BaseModel):
    """Response body for ``POST /jobs``."""

    job_id: str
    status: JobStatus
    queue_message_id: str
    estimated_latency_ms: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "queue_message_id": self.queue_message_id,
            "estimated_latency_ms": self.estimated_latency_ms,
        }


class EnrichmentProgress(BaseModel):
    """Status for individual enrichment pipelines."""

    name: str
    status: EnrichmentStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    detail: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        payload = {"name": self.name, "status": self.status.value}
        if self.started_at:
            payload["started_at"] = self.started_at.strftime(ISO8601_FORMAT)
        if self.completed_at:
            payload["completed_at"] = self.completed_at.strftime(ISO8601_FORMAT)
        if self.detail:
            payload["detail"] = self.detail
        return payload


class JobStatusResponse(BaseModel):
    """Response payload for ``GET /jobs/{job_id}``."""

    job_id: str
    status: JobStatus
    submitted_at: datetime
    updated_at: datetime
    error: Optional[str] = None
    enrichments: List[EnrichmentProgress] = Field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "job_id": self.job_id,
            "status": self.status.value,
            "submitted_at": self.submitted_at.strftime(ISO8601_FORMAT),
            "updated_at": self.updated_at.strftime(ISO8601_FORMAT),
        }
        if self.error:
            payload["error"] = self.error
        if self.enrichments:
            payload["enrichments"] = [enrichment.to_dict() for enrichment in self.enrichments]
        return payload


class JobResultsResponse(BaseModel):
    """Payload returned by ``GET /jobs/{job_id}/results``."""

    job_id: str
    status: JobStatus
    documents: List[Dict[str, Any]]
    next_page_token: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "job_id": self.job_id,
            "status": self.status.value,
            "documents": self.documents,
        }
        if self.next_page_token:
            payload["next_page_token"] = self.next_page_token
        return payload


class CompletionNotificationPayload(BaseModel):
    """Event that is emitted when a job reaches a terminal state."""

    job_id: str
    status: JobStatus
    documents: List[Dict[str, Any]]
    enrichments: List[Dict[str, Any]]
    published_at: datetime

    def to_json(self) -> str:
        payload = {
            "job_id": self.job_id,
            "status": self.status.value,
            "documents": self.documents,
            "enrichments": self.enrichments,
            "published_at": self.published_at.strftime(ISO8601_FORMAT),
        }
        return json.dumps(payload)


def parse_job_status_record(record: Dict[str, Any]) -> JobStatusResponse:
    """Map a DynamoDB/SQL record into :class:`JobStatusResponse`."""

    enrichments = [
        EnrichmentProgress(
            name=enrichment["name"],
            status=EnrichmentStatus(enrichment["status"]),
            started_at=_parse_optional_datetime(enrichment.get("started_at")),
            completed_at=_parse_optional_datetime(enrichment.get("completed_at")),
            detail=enrichment.get("detail"),
        )
        for enrichment in record.get("enrichments", [])
    ]
    return JobStatusResponse(
        job_id=record["job_id"],
        status=JobStatus(record["status"]),
        submitted_at=_parse_datetime(record["submitted_at"]),
        updated_at=_parse_datetime(record["updated_at"]),
        error=record.get("error"),
        enrichments=enrichments,
    )


def _parse_datetime(value: str) -> datetime:
    return datetime.strptime(value, ISO8601_FORMAT)


def _parse_optional_datetime(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    return _parse_datetime(value)
