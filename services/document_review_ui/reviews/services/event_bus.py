"""Publish review outcomes back to the IDP event queue."""
from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict

from django.core.exceptions import ImproperlyConfigured

LOGGER = logging.getLogger(__name__)


class ReviewEventPublisher(ABC):
    """Abstract interface for publishing review results."""

    @abstractmethod
    def publish(self, payload: Dict[str, Any]) -> None:
        """Publish a payload back to the IDP."""


class LoggingReviewEventPublisher(ReviewEventPublisher):
    """Development publisher that only logs review events."""

    def publish(self, payload: Dict[str, Any]) -> None:
        LOGGER.info("Review payload would be sent to queue: %s", json.dumps(payload))


class SqsReviewEventPublisher(ReviewEventPublisher):
    """Publish review events to an AWS SQS queue."""

    def __init__(self) -> None:
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ImproperlyConfigured("boto3 is required to use the SQS event publisher") from exc

        queue_url = os.environ.get("DOCUMENT_REVIEW_SQS_QUEUE_URL")
        if not queue_url:
            raise ImproperlyConfigured("DOCUMENT_REVIEW_SQS_QUEUE_URL must be configured for SQS publisher")
        region = os.environ.get("DOCUMENT_REVIEW_SQS_REGION")
        self._queue_url = queue_url
        self._client = boto3.client("sqs", region_name=region)

    def publish(self, payload: Dict[str, Any]) -> None:
        message_body = json.dumps(payload)
        self._client.send_message(QueueUrl=self._queue_url, MessageBody=message_body)
        LOGGER.info("Dispatched review payload to SQS queue %s", self._queue_url)


def event_publisher_from_env() -> ReviewEventPublisher:
    """Create an event publisher based on environment variables."""

    backend = os.environ.get("DOCUMENT_REVIEW_EVENT_PUBLISHER", "logging").lower()
    if backend == "logging":
        return LoggingReviewEventPublisher()
    if backend == "sqs":
        return SqsReviewEventPublisher()
    raise ImproperlyConfigured(f"Unsupported DOCUMENT_REVIEW_EVENT_PUBLISHER '{backend}'")
