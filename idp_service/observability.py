"""Utilities for structured logging and operational telemetry in Databricks jobs."""

from __future__ import annotations

import logging
import time
import traceback
import uuid
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

try:  # pragma: no cover - optional dependency in local execution
    import boto3
except Exception:  # pragma: no cover - defensive import guard
    boto3 = None  # type: ignore

try:  # pragma: no cover - optional dependency in local execution
    import mlflow
except Exception:  # pragma: no cover - defensive import guard
    mlflow = None  # type: ignore

try:  # pragma: no cover - optional dependency in local execution
    from pyspark.sql import functions as F
except Exception:  # pragma: no cover - defensive import guard
    F = None  # type: ignore

LOGGER = logging.getLogger(__name__)


def _write_delta_records(spark, table: str, records: Iterable[Dict[str, Any]], *, timestamp_column: str) -> None:
    """Persist structured records to a Delta table if Spark is available."""

    records = list(records)
    if not records or spark is None:
        return

    df = spark.createDataFrame(records)
    if F is None:  # pragma: no cover - Spark functions unavailable
        LOGGER.warning("pyspark.sql.functions not available; skipping Delta persistence for %s", table)
        return
    (
        df.withColumn(timestamp_column, F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table)
    )


class StructuredEventLogger(AbstractContextManager["StructuredEventLogger"]):
    """Context manager that records structured lifecycle events for Databricks jobs."""

    def __init__(
        self,
        *,
        spark=None,
        delta_table: Optional[str] = None,
        job_name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._spark = spark
        self._delta_table = delta_table
        self._job_name = job_name
        self._context = context or {}
        self._run_id = str(uuid.uuid4())
        self._start_ts = time.time()
        self._mlflow_run_started = False

    @property
    def run_id(self) -> str:
        return self._run_id

    def __enter__(self) -> "StructuredEventLogger":
        self._start_ts = time.time()
        self.log_event("started")

        if mlflow is not None:
            try:
                active = mlflow.active_run()
                if active is None:
                    mlflow.start_run(run_name=self._job_name)
                    self._mlflow_run_started = True
                mlflow.set_tags({"job_name": self._job_name, **self._context})
            except Exception:  # pragma: no cover - telemetry should not break jobs
                LOGGER.exception("Failed to initialise MLflow run for structured logging")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        duration_ms = int((time.time() - self._start_ts) * 1000)
        if exc_type is not None:
            failure_payload = {
                "status": "failed",
                "duration_ms": duration_ms,
                "error_type": getattr(exc_type, "__name__", str(exc_type)),
                "error_message": str(exc_val),
                "stacktrace": "".join(traceback.format_exception(exc_type, exc_val, exc_tb)),
            }
            self.log_event("completed", **failure_payload)
        else:
            self.log_event("completed", status="succeeded", duration_ms=duration_ms)

        if mlflow is not None and self._mlflow_run_started:
            try:
                mlflow.end_run()
            except Exception:  # pragma: no cover - telemetry should not break jobs
                LOGGER.exception("Failed to close MLflow run for structured logging")
        return False

    def log_event(self, name: str, **fields: Any) -> None:
        """Persist a structured event to Delta/MLflow for downstream analysis."""

        payload = {
            "event_name": name,
            "job_name": self._job_name,
            "run_id": self._run_id,
            "timestamp": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
        }
        payload.update(self._context)
        payload.update(fields)

        if self._spark is not None and self._delta_table:
            try:
                _write_delta_records(self._spark, self._delta_table, [payload], timestamp_column="event_recorded_at")
            except Exception:  # pragma: no cover - telemetry should not break jobs
                LOGGER.exception("Failed to persist structured logging event to Delta table %s", self._delta_table)

        if mlflow is not None:
            try:
                active = mlflow.active_run()
                if active is not None:
                    metrics = {k: v for k, v in payload.items() if isinstance(v, (int, float))}
                    if metrics:
                        mlflow.log_metrics(metrics)
                    tags = {k: str(v) for k, v in payload.items() if k not in metrics and v is not None}
                    if tags:
                        mlflow.set_tags(tags)
            except Exception:  # pragma: no cover - telemetry should not break jobs
                LOGGER.exception("Failed to log structured event to MLflow")


@dataclass
class CloudWatchMetricsEmitter:
    """Thin wrapper for publishing custom metrics to CloudWatch."""

    namespace: str
    region: str
    queue_name: Optional[str] = None

    def __post_init__(self) -> None:
        self._client = None
        if boto3 is None:  # pragma: no cover - optional dependency
            LOGGER.warning("boto3 is not available; CloudWatch metrics will be disabled")
            return
        try:
            self._client = boto3.client("cloudwatch", region_name=self.region)
        except Exception:  # pragma: no cover - telemetry should not break jobs
            LOGGER.exception("Failed to create CloudWatch client; metrics will be disabled")
            self._client = None

    def _emit(self, metric_data: List[Dict[str, Any]]) -> None:
        if not metric_data or self._client is None:
            return
        try:
            self._client.put_metric_data(Namespace=self.namespace, MetricData=metric_data)
        except Exception:  # pragma: no cover - telemetry should not break jobs
            LOGGER.exception("Failed to publish CloudWatch metrics")

    def emit_queue_depth(self, visible: Optional[int], not_visible: Optional[int]) -> None:
        dimensions = self._dimensions()
        metric_data = []
        if visible is not None:
            metric_data.append(
                {
                    "MetricName": "QueueDepthVisible",
                    "Timestamp": datetime.utcnow(),
                    "Value": int(visible),
                    "Unit": "Count",
                    "Dimensions": dimensions,
                }
            )
        if not_visible is not None:
            metric_data.append(
                {
                    "MetricName": "QueueDepthInFlight",
                    "Timestamp": datetime.utcnow(),
                    "Value": int(not_visible),
                    "Unit": "Count",
                    "Dimensions": dimensions,
                }
            )
        self._emit(metric_data)

    def emit_processing_success(self, latency_ms: Optional[float] = None) -> None:
        metric_data = [
            {
                "MetricName": "ParserSuccess",
                "Timestamp": datetime.utcnow(),
                "Value": 1,
                "Unit": "Count",
                "Dimensions": self._dimensions(),
            }
        ]
        if latency_ms is not None:
            metric_data.append(
                {
                    "MetricName": "ParserLatencyMs",
                    "Timestamp": datetime.utcnow(),
                    "Value": float(latency_ms),
                    "Unit": "Milliseconds",
                    "Dimensions": self._dimensions(),
                }
            )
        self._emit(metric_data)

    def emit_processing_failure(self) -> None:
        self._emit(
            [
                {
                    "MetricName": "ParserFailure",
                    "Timestamp": datetime.utcnow(),
                    "Value": 1,
                    "Unit": "Count",
                    "Dimensions": self._dimensions(),
                }
            ]
        )

    def _dimensions(self) -> List[Dict[str, Any]]:
        if self.queue_name:
            return [{"Name": "QueueName", "Value": self.queue_name}]
        return []


__all__ = ["StructuredEventLogger", "CloudWatchMetricsEmitter"]
