"""Databricks notebook entry point for processing batched SQS payloads.

This notebook is designed to run as a Databricks Job with multi-task workflows.
It supports configurable batch sizes, fan-out to worker clusters via the Jobs
API, and persists raw metadata to Delta Lake for monitoring/replay.

Document routing, categorisation, and strategy selection live in the
``databricks.routing`` package, allowing the ingestion loop to orchestrate
production-ready routing strategies without embedding the implementation
directly in this module.
"""

import concurrent.futures
import importlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit

from databricks.routing import (
    DocumentAnalysis,
    DocumentCategory,
    DocumentRouter,
    HeuristicLayoutAnalyser,
    InlineDocumentContentResolver,
    ModelBackedLayoutAnalyser,
    OverrideSet,
    PatternOverride,
    PyMuPDFLayoutAnalyser,
    RequestsLayoutModelClient,
    RouterConfig,
    RoutingMode,
    StrategyConfig,
    LayoutModelType,
)


LOGGER = logging.getLogger(__name__)

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
    category_thresholds: Dict[str, int] = field(default_factory=dict)
    default_strategy_map: Dict[str, Dict[str, Optional[str]]] = field(default_factory=dict)
    delta_override_table: Optional[str] = None
    secrets_scope: Optional[str] = None
    strategy_override_secret: Optional[str] = None
    request_override_flag: str = "parser_override"
    routing_metadata_table: Optional[str] = None
    routing_mode: str = RoutingMode.HYBRID.value
    static_strategy: Optional[Dict[str, Optional[str]]] = None
    layout_model_endpoint: Optional[str] = None
    layout_model_secret_scope: Optional[str] = None
    layout_model_secret_key: Optional[str] = None
    layout_model_timeout_seconds: int = 60
    layout_model_type: Optional[str] = None


# ---------------------------------------------------------------------------
# SQS integration
# ---------------------------------------------------------------------------


def _create_sqs_client(region: str):
    return boto3.client(
        "sqs",
        region_name=region,
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )


def _create_s3_client(region: str):
    return boto3.client(
        "s3",
        region_name=region,
        config=Config(retries={"max_attempts": 5, "mode": "adaptive"}),
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


def _parse_json_env(env_var: str, default):
    value = os.environ.get(env_var)
    if not value:
        return default

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return default

    return parsed if parsed is not None else default


def persist_metadata(spark: SparkSession, metadata_table: str, records: List[dict]):
    if not records:
        return

    df: DataFrame = spark.createDataFrame(records)

    (df.withColumn("ingested_at", current_timestamp())
       .write
       .format("delta")
       .mode("append")
       .saveAsTable(metadata_table))


def _resolve_default_strategy_map(config: IngestionConfig) -> Dict[str, Dict[str, Optional[str]]]:
    if config.default_strategy_map:
        return config.default_strategy_map

    return {
        DocumentCategory.SHORT_FORM.value: {"name": "general", "model": None},
        DocumentCategory.LONG_FORM.value: {"name": "custom_model", "model": "longform-v1"},
        DocumentCategory.SCANNED.value: {"name": "ocr_enhanced", "model": "ocr-2024"},
        DocumentCategory.TABLE_HEAVY.value: {"name": "table_extractor", "model": "tabular-v2"},
        DocumentCategory.FORM_HEAVY.value: {"name": "forms_extractor", "model": "forms-v1"},
        DocumentCategory.UNKNOWN.value: {"name": "fallback_non_azure", "model": None},
    }


def _build_router_config(config: IngestionConfig) -> RouterConfig:
    default_map = _resolve_default_strategy_map(config)
    fallback_entry = default_map.get(DocumentCategory.UNKNOWN.value)
    static_strategy = config.static_strategy
    if isinstance(static_strategy, str):
        static_strategy = {"name": static_strategy}
    return RouterConfig(
        mode=config.routing_mode,
        request_override_flag=config.request_override_flag,
        category_thresholds=config.category_thresholds,
        default_strategy_map=default_map,
        fallback_strategy=fallback_entry,
        static_strategy=static_strategy,
    )


def _get_workspace_client():
    spec = importlib.util.find_spec("databricks.sdk")
    if spec is None:
        return None

    sdk_module = importlib.import_module("databricks.sdk")
    workspace_client_cls = getattr(sdk_module, "WorkspaceClient", None)
    if workspace_client_cls is None:
        return None

    try:
        return workspace_client_cls()
    except Exception:  # pragma: no cover - defensive
        LOGGER.exception("Unable to instantiate databricks.sdk.WorkspaceClient")
        return None


def _build_layout_analyser(
    config: IngestionConfig,
    workspace_client,
    content_resolvers,
):
    heuristic = HeuristicLayoutAnalyser()
    pymupdf = PyMuPDFLayoutAnalyser(content_resolvers=content_resolvers, fallback=heuristic)

    if not config.layout_model_endpoint:
        return pymupdf

    api_key = None
    scope = config.layout_model_secret_scope or config.secrets_scope
    if scope and config.layout_model_secret_key and workspace_client:
        try:
            api_key = workspace_client.secrets.get(
                scope=scope,
                key=config.layout_model_secret_key,
            ).value
        except Exception:
            LOGGER.exception(
                "Failed to retrieve layout model secret from scope %s", scope
            )

    model_type = None
    if config.layout_model_type:
        try:
            model_type = LayoutModelType(config.layout_model_type)
        except ValueError:
            LOGGER.warning(
                "Unknown layout model type '%s'; defaulting to endpoint defaults",
                config.layout_model_type,
            )

    client = RequestsLayoutModelClient(
        config.layout_model_endpoint,
        api_key=api_key,
        timeout_seconds=config.layout_model_timeout_seconds,
        model_type=model_type,
    )
    return ModelBackedLayoutAnalyser(client, fallback=pymupdf)


def _build_content_resolvers(config: IngestionConfig, s3_client) -> List[object]:
    resolvers: List[object] = [InlineDocumentContentResolver()]
    if s3_client:
        resolvers.append(S3ContentResolver(s3_client))
    return resolvers


def _resolve_object_key(body: dict) -> Optional[str]:
    candidates = [
        body.get("s3", {}).get("object", {}).get("key") if isinstance(body, dict) else None,
        body.get("object_key") if isinstance(body, dict) else None,
        body.get("objectKey") if isinstance(body, dict) else None,
        body.get("source_path") if isinstance(body, dict) else None,
    ]
    for candidate in candidates:
        if candidate:
            return candidate
    return None


def _pattern_override_from_mapping(payload: dict) -> Optional[PatternOverride]:
    if not isinstance(payload, dict):
        return None

    pattern = payload.get("pattern") or payload.get("document_pattern")
    strategy_name = payload.get("strategy") or payload.get("name")
    if not pattern or not strategy_name:
        return None

    try:
        compiled = re.compile(pattern)
    except re.error:
        LOGGER.warning("Invalid override regex: %s", pattern)
        return None

    strategy_payload = {
        "name": strategy_name,
        "model": payload.get("model"),
        "max_pages": payload.get("max_pages"),
    }
    strategy = StrategyConfig.from_mapping(strategy_payload)
    return PatternOverride(pattern=compiled, strategy=strategy)


class S3ContentResolver:
    """Downloads document bytes from S3 for rich layout analysis."""

    def __init__(self, s3_client, max_bytes: int = 20 * 1024 * 1024) -> None:
        self.s3_client = s3_client
        self.max_bytes = max_bytes

    def fetch(self, descriptor) -> Optional[bytes]:  # Protocol compatible with DocumentRouter
        bucket = getattr(descriptor, "bucket", None)
        key = getattr(descriptor, "object_key", None)
        if not bucket or not key:
            return None

        try:
            request_kwargs = {"Bucket": bucket, "Key": key}
            if self.max_bytes:
                request_kwargs["Range"] = f"bytes=0-{self.max_bytes - 1}"
            response = self.s3_client.get_object(**request_kwargs)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code") if exc.response else None
            if error_code not in {"NoSuchKey", "404"}:
                LOGGER.warning("Unable to download document %s/%s for analysis", bucket, key)
            return None
        body = response.get("Body")
        if body is None:
            return None
        try:
            return body.read(self.max_bytes)
        finally:
            try:
                body.close()
            except Exception:
                pass


class ConfiguredOverrideProvider:
    """Loads overrides from secrets, Delta tables, and environment variables."""

    def __init__(self, spark: SparkSession, config: IngestionConfig, workspace_client) -> None:
        self.spark = spark
        self.config = config
        self.workspace_client = workspace_client

    def load(self) -> OverrideSet:
        pattern_overrides: List[PatternOverride] = []
        pattern_overrides.extend(self._from_secret())
        pattern_overrides.extend(self._from_delta_table())
        pattern_overrides.extend(self._from_environment())
        return OverrideSet(pattern_overrides=pattern_overrides)

    def _from_secret(self) -> List[PatternOverride]:
        if not (self.config.secrets_scope and self.config.strategy_override_secret and self.workspace_client):
            return []

        try:
            secret_value = self.workspace_client.secrets.get(
                scope=self.config.secrets_scope,
                key=self.config.strategy_override_secret,
            ).value
        except Exception:
            LOGGER.exception(
                "Failed to load parser strategy overrides from secret %s/%s",
                self.config.secrets_scope,
                self.config.strategy_override_secret,
            )
            return []

        return self._decode_override_payload(secret_value, source="secret")

    def _from_delta_table(self) -> List[PatternOverride]:
        if not self.config.delta_override_table:
            return []

        overrides: List[PatternOverride] = []
        try:
            table_df = self.spark.table(self.config.delta_override_table)
            for row in table_df.collect():
                payload = {
                    "pattern": getattr(row, "document_pattern", None) or getattr(row, "pattern", None),
                    "strategy": getattr(row, "strategy", None),
                    "max_pages": getattr(row, "max_pages", None),
                    "model": getattr(row, "model", None),
                }
                override = _pattern_override_from_mapping(payload)
                if override:
                    overrides.append(override)
        except Exception:
            LOGGER.exception(
                "Failed to read parser strategy overrides from table %s",
                self.config.delta_override_table,
            )
        return overrides

    def _from_environment(self) -> List[PatternOverride]:
        env_payload = os.environ.get("PARSER_STRATEGY_OVERRIDES")
        if not env_payload:
            return []
        return self._decode_override_payload(env_payload, source="environment")

    def _decode_override_payload(self, payload: str, source: str) -> List[PatternOverride]:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            LOGGER.warning("Invalid override payload from %s", source)
            return []

        candidates: Iterable[dict]
        if isinstance(data, list):
            candidates = data
        elif isinstance(data, dict) and "pattern_overrides" in data:
            candidates = data.get("pattern_overrides", [])
        else:
            candidates = [data]

        overrides: List[PatternOverride] = []
        for candidate in candidates:
            override = _pattern_override_from_mapping(candidate)
            if override:
                overrides.append(override)
        return overrides


def _routing_record_from_analysis(analysis: DocumentAnalysis) -> dict:
    return {
        "source_path": analysis.object_key,
        "routing": json.dumps(
            {
                "category": analysis.category.value,
                "strategy": analysis.strategy.name,
                "reason": analysis.strategy.reason,
                "model": analysis.strategy.model,
                "max_pages": analysis.strategy.max_pages,
                "overrides": analysis.overrides_applied,
                "request_override": analysis.request_override,
                "average_text_density": analysis.average_text_density,
                "average_image_density": analysis.average_image_density,
                "table_page_ratio": analysis.table_page_ratio,
                "scanned_page_ratio": analysis.scanned_page_ratio,
                "checkbox_page_ratio": analysis.checkbox_page_ratio,
                "radio_button_page_ratio": analysis.radio_button_page_ratio,
                "form_page_ratio": analysis.form_page_ratio,
                "total_tables": analysis.total_tables,
                "total_checkboxes": analysis.total_checkboxes,
                "total_radio_buttons": analysis.total_radio_buttons,
                "page_metrics": [page.to_dict() for page in analysis.pages],
            }
        ),
    }


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
    try:
        s3_client = _create_s3_client(config.region)
    except Exception:
        LOGGER.exception("Failed to construct S3 client for document download")
        s3_client = None
    workspace_client = _get_workspace_client()
    router_config = _build_router_config(config)
    content_resolvers = _build_content_resolvers(config, s3_client)
    layout_analyser = _build_layout_analyser(config, workspace_client, content_resolvers)
    router = DocumentRouter(
        router_config,
        layout_analyser,
        content_resolvers=content_resolvers,
    )
    override_provider = ConfiguredOverrideProvider(spark, config, workspace_client)
    batches_processed = 0
    total_messages = 0

    while True:
        overrides = override_provider.load()
        messages = receive_message_batch(sqs_client, config.queue_url, config)
        if not messages:
            time.sleep(config.poll_interval_seconds)
            continue

        receipt_handles = [message["ReceiptHandle"] for message in messages]
        message_payloads = [json.loads(message["Body"]) for message in messages]

        metadata_records: List[dict] = []
        analyses: List[DocumentAnalysis] = []
        routed_payload: List[dict] = []
        for message, body in zip(messages, message_payloads):
            object_key = _resolve_object_key(body)
            if not object_key:
                LOGGER.warning("Skipping message %s without object key", message["MessageId"])
                continue
            base_record = {
                "source_path": object_key,
                "file_type": os.path.splitext(object_key or "")[1].lstrip("."),
                "message_id": message["MessageId"],
                "sns_topic": body.get("TopicArn"),
                "queue_url": config.queue_url,
            }

            analysis = router.route(body, object_key, overrides)
            analyses.append(analysis)
            metadata_records.append(analysis.to_metadata_record(base_record))
            routed_payload.append(body)

        persist_metadata(spark, config.metadata_table, metadata_records)

        if config.routing_metadata_table:
            routing_records = [_routing_record_from_analysis(analysis) for analysis in analyses]
            persist_metadata(spark, config.routing_metadata_table, routing_records)

        if config.dispatch_job_id and routed_payload:
            dispatch_to_worker(
                config.dispatch_job_id,
                routed_payload,
                task_parameters=config.worker_task_parameters,
            )
        elif not config.dispatch_job_id and routed_payload:
            # Inline processing placeholder: replace with domain-specific logic.
            with concurrent.futures.ThreadPoolExecutor() as executor:
                list(executor.map(lambda body: body, routed_payload))

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
        worker_task_parameters=_parse_json_env("WORKER_TASK_PARAMETERS", {}),
        metadata_table=os.environ.get("METADATA_TABLE", "lakehouse.raw_ingestion_metadata"),
        category_thresholds=_parse_json_env("CATEGORY_THRESHOLDS", {}),
        default_strategy_map=_parse_json_env("DEFAULT_STRATEGY_MAP", {}),
        delta_override_table=os.environ.get("DELTA_OVERRIDE_TABLE"),
        secrets_scope=os.environ.get("STRATEGY_SECRETS_SCOPE"),
        strategy_override_secret=os.environ.get("STRATEGY_OVERRIDE_SECRET"),
        request_override_flag=os.environ.get("REQUEST_OVERRIDE_FLAG", "parser_override"),
        routing_metadata_table=os.environ.get("ROUTING_METADATA_TABLE"),
        routing_mode=os.environ.get("ROUTING_MODE", RoutingMode.HYBRID.value),
        static_strategy=_parse_json_env("STATIC_ROUTING_STRATEGY", None),
        layout_model_endpoint=os.environ.get("LAYOUT_MODEL_ENDPOINT"),
        layout_model_secret_scope=os.environ.get("LAYOUT_MODEL_SECRET_SCOPE"),
        layout_model_secret_key=os.environ.get("LAYOUT_MODEL_SECRET_KEY"),
        layout_model_timeout_seconds=int(os.environ.get("LAYOUT_MODEL_TIMEOUT_SECONDS", "60")),
    )

    process_messages(config)
