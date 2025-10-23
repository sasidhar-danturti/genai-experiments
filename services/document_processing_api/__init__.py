"""Serverless API surface for document processing workflows."""

from .contracts import (
    CompletionNotificationPayload,
    EnrichmentStatus,
    JobResultsResponse,
    JobStatus,
    JobStatusResponse,
    NotificationConfig,
    SubmitJobRequest,
    SubmitJobResponse,
)
from .handlers import (
    fetch_results_handler,
    get_job_status_handler,
    submit_job_handler,
)

__all__ = [
    "CompletionNotificationPayload",
    "EnrichmentStatus",
    "JobResultsResponse",
    "JobStatus",
    "JobStatusResponse",
    "NotificationConfig",
    "SubmitJobRequest",
    "SubmitJobResponse",
    "fetch_results_handler",
    "get_job_status_handler",
    "submit_job_handler",
]
