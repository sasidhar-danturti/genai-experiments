"""Notification dispatchers for completion events."""

from __future__ import annotations

import logging
from typing import Any, Dict
from urllib.request import Request, urlopen

try:  # pragma: no cover - boto3 may be unavailable in unit test environments.
    import boto3
except Exception:  # pragma: no cover
    boto3 = None

from .contracts import CompletionNotificationPayload, NotificationConfig

LOGGER = logging.getLogger(__name__)


def dispatch_completion_notifications(
    job_record: Dict[str, Any], payload: CompletionNotificationPayload
) -> None:
    """Send completion events to the destinations configured on the job."""

    config_dict = job_record.get("notification_config")
    if not config_dict:
        LOGGER.debug("No notification config recorded for job %s", job_record.get("job_id"))
        return

    config = NotificationConfig(
        sns_topic_arn=config_dict.get("sns_topic_arn"),
        webhook_url=config_dict.get("webhook_url"),
        include_enrichment_events=config_dict.get("include_enrichment_events", True),
    )

    payload_to_send = payload
    if not config.include_enrichment_events and payload.enrichments:
        payload_to_send = CompletionNotificationPayload(
            job_id=payload.job_id,
            status=payload.status,
            documents=payload.documents,
            enrichments=[],
            published_at=payload.published_at,
        )

    if config.sns_topic_arn:
        _publish_to_sns(config.sns_topic_arn, payload_to_send)
    if config.webhook_url:
        _invoke_webhook(config.webhook_url, payload_to_send)


def _publish_to_sns(topic_arn: str, payload: CompletionNotificationPayload) -> None:
    if boto3 is None:  # pragma: no cover
        raise RuntimeError("boto3 is required to publish SNS notifications")

    LOGGER.info("Publishing completion notification for job %s to SNS", payload.job_id)
    client = boto3.client("sns")
    client.publish(TopicArn=topic_arn, Message=payload.to_json())


def _invoke_webhook(url: str, payload: CompletionNotificationPayload) -> None:
    LOGGER.info("Invoking webhook for job %s", payload.job_id)
    data = payload.to_json().encode("utf-8")
    request = Request(url, data=data, headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=10) as response:  # nosec B310
        response.read()
