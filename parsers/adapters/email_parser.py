"""Adapter that normalises raw email payloads including attachments."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from ..canonical_schema import (
    CanonicalDocument,
    CanonicalTextSpan,
    ConfidenceSignal,
    DocumentAttachment,
    ExtractionProvenance,
    PageSegment,
    StructuredField,
)
from .base import AdapterError, ParserAdapter


class EmailParserAdapter(ParserAdapter):
    """Create canonical documents from parsed email data structures."""

    def transform(
        self,
        payload: Any,
        *,
        document_id: str,
        source_uri: str,
        checksum: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CanonicalDocument:
        payload_dict = self._ensure_mapping(payload)

        metadata_payload: Dict[str, Any] = {"provider": "email_parser", **(payload_dict.get("metadata") or {}), **(metadata or {})}
        for key in ("subject", "from", "to", "cc", "bcc", "sent_at"):
            if key in payload_dict and key not in metadata_payload:
                metadata_payload[key] = payload_dict[key]

        text_spans = list(self._build_text_spans(payload_dict))
        fields = list(self._build_header_fields(payload_dict))
        attachments = list(self._build_attachments(payload_dict.get("attachments")))

        document_type = metadata_payload.get("document_type") or "email"
        mime_type = metadata_payload.get("mime_type") or "message/rfc822"

        page_segments = [
            PageSegment(
                page_number=1,
                parser="email_parser",
                method="message",
            )
        ]

        return CanonicalDocument(
            document_id=document_id,
            source_uri=source_uri,
            checksum=checksum,
            text_spans=text_spans,
            tables=[],
            fields=fields,
            visual_descriptions=[],
            page_segments=page_segments,
            attachments=attachments,
            document_type=document_type,
            mime_type=mime_type,
            metadata=metadata_payload,
        )

    # ------------------------------------------------------------------
    # Builders
    # ------------------------------------------------------------------

    def _build_text_spans(self, payload: Dict[str, Any]) -> Iterable[CanonicalTextSpan]:
        body_text = payload.get("body_text") or payload.get("text")
        if isinstance(body_text, str) and body_text.strip():
            yield self._span_from_text(body_text, method="body_text", span_id="body-text")

        supplemental_spans = payload.get("text_spans") or []
        for idx, span in enumerate(supplemental_spans):
            if not isinstance(span, dict):
                continue
            content = span.get("content") or span.get("text")
            if not content:
                continue
            confidence = self._normalise_confidence(span.get("confidence"))
            provenance = ExtractionProvenance(
                parser="email_parser",
                method=span.get("method") or "body_segment",
            )
            confidence_signals = [
                ConfidenceSignal(
                    source="email_parser",
                    confidence=confidence,
                    method=provenance.method,
                )
            ]
            yield CanonicalTextSpan(
                content=str(content),
                confidence=confidence,
                span_id=str(span.get("id") or f"email-span-{idx}"),
                provenance=provenance,
                confidence_signals=confidence_signals,
            )

    def _build_header_fields(self, payload: Dict[str, Any]) -> Iterable[StructuredField]:
        headers = payload.get("headers") or {}
        for name, value in headers.items():
            confidence = self._normalise_confidence(1.0)
            provenance = ExtractionProvenance(parser="email_parser", method="header")
            confidence_signals = [
                ConfidenceSignal(source="email_parser", confidence=confidence, method="header")
            ]
            yield StructuredField(
                name=str(name),
                value=None if value is None else str(value),
                confidence=confidence,
                value_type="header",
                provenance=provenance,
                confidence_signals=confidence_signals,
            )

        entities = payload.get("entities") or []
        for idx, entity in enumerate(entities):
            if not isinstance(entity, dict):
                continue
            name = entity.get("name") or entity.get("label") or f"entity-{idx}"
            value = entity.get("value") or entity.get("text")
            confidence = self._normalise_confidence(entity.get("confidence"))
            provenance = ExtractionProvenance(
                parser="email_parser",
                method=entity.get("method") or "entity",
            )
            confidence_signals = [
                ConfidenceSignal(
                    source="email_parser",
                    confidence=confidence,
                    method=provenance.method,
                )
            ]
            yield StructuredField(
                name=str(name),
                value=None if value is None else str(value),
                confidence=confidence,
                value_type=entity.get("type") or "entity",
                provenance=provenance,
                confidence_signals=confidence_signals,
            )

    def _build_attachments(self, attachments: Any) -> Iterable[DocumentAttachment]:
        for idx, attachment in enumerate(attachments or []):
            if not isinstance(attachment, dict):
                continue
            attachment_id = attachment.get("attachment_id") or attachment.get("id") or f"attachment-{idx}"
            file_name = attachment.get("file_name") or attachment.get("name")
            mime_type = attachment.get("mime_type") or attachment.get("content_type")
            if not file_name or not mime_type:
                continue
            checksum = attachment.get("checksum")
            source_uri = attachment.get("source_uri")
            metadata = attachment.get("metadata") or {}
            canonical_document = attachment.get("canonical_document")
            if canonical_document is not None and not isinstance(canonical_document, CanonicalDocument):
                metadata = {**metadata, "canonical_document": canonical_document}
                canonical_document = None
            yield DocumentAttachment(
                attachment_id=str(attachment_id),
                file_name=str(file_name),
                mime_type=str(mime_type),
                checksum=checksum,
                source_uri=source_uri,
                document=canonical_document,
                metadata=metadata,
            )

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _ensure_mapping(payload: Any) -> Dict[str, Any]:
        if payload is None:
            raise AdapterError("Email payload is empty")
        if not isinstance(payload, dict):
            raise AdapterError("Email payload must be a mapping")
        return payload

    def _span_from_text(self, text: str, *, method: str, span_id: str) -> CanonicalTextSpan:
        confidence = 1.0
        provenance = ExtractionProvenance(parser="email_parser", method=method)
        confidence_signal = ConfidenceSignal(source="email_parser", confidence=confidence, method=method)
        return CanonicalTextSpan(
            content=text,
            confidence=confidence,
            span_id=span_id,
            provenance=provenance,
            confidence_signals=[confidence_signal],
        )
