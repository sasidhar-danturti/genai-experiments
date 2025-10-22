"""Databricks workflow wrapper for Azure Document Intelligence processing."""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field, replace
from typing import Any, Dict, Iterable, Optional, Protocol

from parsers.adapters import AzureDocumentIntelligenceAdapter, ParserAdapter
from parsers.canonical_schema import CanonicalDocument
from .summarization import DefaultDocumentSummarizer, DocumentSummarizer

logger = logging.getLogger(__name__)


class AnalyzePoller(Protocol):
    """Protocol describing the poller returned by Azure SDK calls."""

    def result(self) -> Any:
        ...


class AzureDocumentIntelligenceClient(Protocol):
    """Subset of the Azure Document Intelligence client used by the workflow."""

    def begin_analyze_document(self, model_id: str, document: Any, **kwargs: Any) -> AnalyzePoller:
        ...


class DocumentResultStore(Protocol):
    """Store used to provide idempotent writes for document results."""

    def has_record(self, document_id: str, checksum: str) -> bool:
        ...

    def save(self, result: CanonicalDocument) -> None:
        ...


@dataclass(frozen=True)
class WorkflowConfig:
    """Configuration for the Azure Document Intelligence workflow."""

    model_id: str
    max_retries: int = 3
    retry_backoff_seconds: float = 5.0
    adapter: ParserAdapter = field(default_factory=AzureDocumentIntelligenceAdapter)
    summarizer: DocumentSummarizer = field(default_factory=DefaultDocumentSummarizer)


class AzureDocumentIntelligenceService:
    """Small wrapper around the Azure SDK with retry semantics."""

    def __init__(self, client: AzureDocumentIntelligenceClient, config: WorkflowConfig):
        self._client = client
        self._config = config

    def analyze(self, document: Any, *, pages: Optional[Iterable[int]] = None, content_type: Optional[str] = None) -> Any:
        attempt = 0
        while True:
            try:
                logger.debug("Submitting document to Azure Document Intelligence", extra={"pages": pages})
                poller = self._client.begin_analyze_document(
                    self._config.model_id,
                    document,
                    **_build_request_kwargs(pages=pages, content_type=content_type),
                )
                return poller.result()
            except Exception as exc:  # pragma: no cover - defensive
                attempt += 1
                if attempt > self._config.max_retries:
                    logger.exception("Azure Document Intelligence analysis failed after retries")
                    raise
                sleep_for = self._config.retry_backoff_seconds * attempt
                logger.warning("Azure Document Intelligence call failed, retrying", exc_info=exc)
                time.sleep(sleep_for)


@dataclass
class WorkflowResult:
    """Outcome of a workflow execution."""

    document: Optional[CanonicalDocument]
    skipped: bool


class DocumentIntelligenceWorkflow:
    """Idempotent workflow to execute Azure Document Intelligence parsing."""

    def __init__(
        self,
        *,
        client: AzureDocumentIntelligenceClient,
        store: DocumentResultStore,
        config: WorkflowConfig,
    ) -> None:
        self._service = AzureDocumentIntelligenceService(client, config)
        self._store = store
        self._config = config
        self._adapter = config.adapter
        self._summarizer = config.summarizer

    def process(
        self,
        *,
        document_id: str,
        document_bytes: bytes,
        source_uri: str,
        metadata: Optional[Dict[str, Any]] = None,
        pages: Optional[Iterable[int]] = None,
        content_type: Optional[str] = None,
        force: bool = False,
    ) -> WorkflowResult:
        checksum = _checksum(document_bytes)
        metadata = metadata or {}

        if not force and self._store.has_record(document_id, checksum):
            logger.info("Skipping document because an identical payload already exists", extra={"document_id": document_id})
            return WorkflowResult(document=None, skipped=True)

        analyze_result = self._service.analyze(
            document_bytes,
            pages=pages,
            content_type=content_type,
        )

        canonical = self._adapter.transform(
            analyze_result,
            document_id=document_id,
            source_uri=source_uri,
            checksum=checksum,
            metadata=metadata,
        )

        if self._summarizer is not None:
            summaries = self._summarizer.summarise(canonical)
            if summaries:
                canonical = replace(
                    canonical,
                    summaries=list(canonical.summaries) + summaries,
                )

        self._store.save(canonical)
        return WorkflowResult(document=canonical, skipped=False)


def _checksum(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _build_request_kwargs(*, pages: Optional[Iterable[int]], content_type: Optional[str]) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {}
    if pages is not None:
        kwargs["pages"] = list(pages)
    if content_type is not None:
        kwargs["content_type"] = content_type
    return kwargs
