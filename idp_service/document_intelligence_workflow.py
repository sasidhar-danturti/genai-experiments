"""Databricks workflow wrapper for Azure Document Intelligence processing."""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from email import message_from_bytes
from email.message import Message
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence

from parsers.adapters import AzureDocumentIntelligenceAdapter, ParserAdapter
from parsers.canonical_schema import (
    CanonicalDocument,
    DocumentAttachment,
    DocumentEnrichment,
)
from parsers.denormalized import canonical_to_denorm_records, DenormRecord
from .enrichment import EnrichmentDispatcher, EnrichmentProvider
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
    enrichment_providers: Sequence[EnrichmentProvider] = field(default_factory=tuple)


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
    records: List[DenormRecord] = field(default_factory=list)


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
        self._enrichment_dispatcher = (
            EnrichmentDispatcher(config.enrichment_providers)
            if config.enrichment_providers
            else None
        )

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
        enrich_with: Optional[Iterable[str]] = None,
    ) -> WorkflowResult:
        checksum = _checksum(document_bytes)
        metadata = metadata or {}

        if not force and self._store.has_record(document_id, checksum):
            logger.info("Skipping document because an identical payload already exists", extra={"document_id": document_id})
            return WorkflowResult(document=None, skipped=True, raw_result=None, records=[])

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

        canonical = self._attach_email_children(
            canonical,
            document_bytes=document_bytes,
            source_uri=source_uri,
            metadata=metadata,
        )

        if self._summarizer is not None:
            summaries = self._summarizer.summarise(canonical)
            if summaries:
                canonical = canonical.model_copy(
                    update={"summaries": list(canonical.summaries) + summaries}
                )

        if self._enrichment_dispatcher is not None and enrich_with:
            enrichment_requests = list(enrich_with)
            if enrichment_requests:
                enrichment_map = self._enrichment_dispatcher.dispatch(
                    [canonical], enrichment_requests
                )
                enrichments: List[DocumentEnrichment] = enrichment_map.get(
                    canonical.document_id, []
                )
                if enrichments:
                    canonical = canonical.model_copy(
                        update={"enrichments": list(canonical.enrichments) + list(enrichments)}
                    )

        self._store.save(canonical)
        records = canonical_to_denorm_records(
            canonical,
            request_id=str(metadata.get("request_id", document_id)),
            generated_at=datetime.now(timezone.utc),
        )
        return WorkflowResult(document=canonical, skipped=False, records=records)

    # ------------------------------------------------------------------
    # Attachment handling
    # ------------------------------------------------------------------

    def _attach_email_children(
        self,
        canonical: CanonicalDocument,
        *,
        document_bytes: bytes,
        source_uri: str,
        metadata: Dict[str, Any],
        depth: int = 0,
    ) -> CanonicalDocument:
        if canonical.attachments or depth > 3:
            return canonical

        mime_type = (metadata.get("mime_type") or canonical.mime_type or "").lower()
        if not mime_type.startswith("message/"):
            return canonical

        try:
            message = message_from_bytes(document_bytes)
        except Exception:  # pragma: no cover - defensive
            logger.warning("Unable to parse email payload for attachments", exc_info=True)
            return canonical

        if not isinstance(message, Message):
            return canonical

        attachments: List[DocumentAttachment] = []
        for index, part in enumerate(message.walk()):
            if part.get_content_disposition() != "attachment":
                continue
            payload = part.get_payload(decode=True) or b""
            if not payload:
                continue

            attachment_filename = part.get_filename() or f"attachment-{index + 1}"
            attachment_mime = part.get_content_type() or "application/octet-stream"
            attachment_checksum = _checksum(payload)
            attachment_document_id = f"{canonical.document_id}::attachment-{index + 1}"
            attachment_source = f"{source_uri}#attachment/{attachment_filename}"

            attachment_metadata: Dict[str, Any] = {
                "mime_type": attachment_mime,
                "parent_document_id": canonical.document_id,
                "attachment_file_name": attachment_filename,
                "content_id": part.get("Content-ID"),
            }

            analyze_result = self._service.analyze(
                payload,
                content_type=attachment_mime,
            )

            attachment_document = self._adapter.transform(
                analyze_result,
                document_id=attachment_document_id,
                source_uri=attachment_source,
                checksum=attachment_checksum,
                metadata=attachment_metadata,
            )

            if attachment_mime.startswith("message/"):
                attachment_document = self._attach_email_children(
                    attachment_document,
                    document_bytes=payload,
                    source_uri=attachment_source,
                    metadata=attachment_metadata,
                    depth=depth + 1,
                )

            attachments.append(
                DocumentAttachment(
                    attachment_id=str(index + 1),
                    file_name=attachment_filename,
                    mime_type=attachment_mime,
                    checksum=attachment_checksum,
                    source_uri=attachment_source,
                    document=attachment_document,
                    metadata={
                        "size_bytes": len(payload),
                        "content_id": part.get("Content-ID"),
                    },
                )
            )

        if not attachments:
            return canonical

        return canonical.model_copy(update={"attachments": list(canonical.attachments) + attachments})


def _checksum(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _build_request_kwargs(*, pages: Optional[Iterable[int]], content_type: Optional[str]) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {}
    if pages is not None:
        kwargs["pages"] = list(pages)
    if content_type is not None:
        kwargs["content_type"] = content_type
    return kwargs
