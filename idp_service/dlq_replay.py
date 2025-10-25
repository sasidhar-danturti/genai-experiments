"""Utilities for replaying messages from SQS dead-letter queues."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Iterable, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

LOGGER = logging.getLogger(__name__)


def _create_client(region: str):
    return boto3.client(
        "sqs",
        region_name=region,
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )


def peek_dlq_messages(
    *,
    dlq_url: str,
    region: str,
    limit: int = 10,
    wait_time_seconds: int = 2,
) -> List[dict]:
    """Inspect a limited number of DLQ messages without deleting them."""

    client = _create_client(region)
    messages: List[dict] = []
    remaining = limit
    while remaining > 0:
        batch_size = min(remaining, 10)
        response = client.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=batch_size,
            WaitTimeSeconds=wait_time_seconds,
            AttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
            MessageAttributeNames=["All"],
        )
        batch = response.get("Messages", [])
        if not batch:
            break
        messages.extend(batch)
        remaining -= len(batch)
        for message in batch:
            client.change_message_visibility(QueueUrl=dlq_url, ReceiptHandle=message["ReceiptHandle"], VisibilityTimeout=0)
    return messages


def replay_dead_letter_queue(
    *,
    dlq_url: str,
    target_queue_url: str,
    region: str,
    limit: Optional[int] = None,
    batch_size: int = 10,
    wait_time_seconds: int = 2,
    throttle_seconds: float = 0.0,
) -> int:
    """Replay DLQ messages into the primary queue."""

    client = _create_client(region)
    replayed = 0
    while True:
        if limit is not None and replayed >= limit:
            break

        request_batch_size = min(batch_size, 10)
        response = client.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=request_batch_size,
            WaitTimeSeconds=wait_time_seconds,
            AttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
            MessageAttributeNames=["All"],
        )
        messages = response.get("Messages", [])
        if not messages:
            break

        for message in messages:
            if limit is not None and replayed >= limit:
                break

            try:
                client.send_message(
                    QueueUrl=target_queue_url,
                    MessageBody=message["Body"],
                    MessageAttributes=message.get("MessageAttributes", {}),
                )
            except ClientError:
                LOGGER.exception("Failed to replay message %s", message.get("MessageId"))
                continue

            client.delete_message(QueueUrl=dlq_url, ReceiptHandle=message["ReceiptHandle"])
            replayed += 1
            if throttle_seconds:
                time.sleep(throttle_seconds)

        if throttle_seconds and replayed:
            time.sleep(throttle_seconds)

    return replayed


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay SQS dead-letter queue messages")
    parser.add_argument("dlq_url", help="URL of the DLQ to drain")
    parser.add_argument("target_queue_url", help="Target queue URL to replay messages into")
    parser.add_argument("--region", default="us-east-1", help="AWS region for the queues")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of messages to replay")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of messages to read per poll")
    parser.add_argument("--wait-time", type=int, default=2, help="Wait time for long polling (seconds)")
    parser.add_argument("--throttle", type=float, default=0.0, help="Delay between message replays (seconds)")
    parser.add_argument("--dry-run", action="store_true", help="Only print DLQ messages without replaying")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    _configure_logging(args.verbose)

    if args.dry_run:
        messages = peek_dlq_messages(
            dlq_url=args.dlq_url,
            region=args.region,
            limit=args.limit or 10,
        )
        for message in messages:
            LOGGER.info("DLQ message %s: %s", message.get("MessageId"), message.get("Body"))
        LOGGER.info("Dry-run complete. %s messages inspected.", len(messages))
        return 0

    replayed = replay_dead_letter_queue(
        dlq_url=args.dlq_url,
        target_queue_url=args.target_queue_url,
        region=args.region,
        limit=args.limit,
        batch_size=args.batch_size,
        wait_time_seconds=args.wait_time,
        throttle_seconds=args.throttle,
    )
    LOGGER.info("Replayed %s DLQ messages into %s", replayed, args.target_queue_url)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
