"""Adapter that ensembles multiple parser outputs into a single canonical document."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..canonical_schema import CanonicalDocument, DocumentAttachment, PageSegment
from .base import AdapterError, ParserAdapter


class MultiParserAdapter(ParserAdapter):
    """Invoke multiple adapters and merge their canonical outputs."""

    def __init__(self, adapters: Dict[str, ParserAdapter]):
        if not adapters:
            raise ValueError("MultiParserAdapter requires at least one adapter")
        self._adapters = dict(adapters)

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
        parser_payloads = payload_dict.get("parsers")
        if not isinstance(parser_payloads, list) or not parser_payloads:
            raise AdapterError("MultiParser payload must contain a non-empty 'parsers' list")

        shared_metadata = dict(payload_dict.get("document_metadata") or {})
        if metadata:
            shared_metadata.update(metadata)

        combined_text_spans = []
        combined_tables = []
        combined_fields = []
        combined_visuals = []
        combined_segments: List[PageSegment] = []
        combined_attachments: List[DocumentAttachment] = []
        combined_summaries = []
        parsers_used: List[str] = []

        document_type = shared_metadata.get("document_type")
        mime_type = shared_metadata.get("mime_type")

        for parser_entry in parser_payloads:
            if not isinstance(parser_entry, dict):
                continue
            name = parser_entry.get("name")
            if not name:
                raise AdapterError("Each parser entry must include a 'name'")
            adapter = self._adapters.get(name)
            if adapter is None:
                raise AdapterError(f"No adapter registered for parser '{name}'")

            entry_payload = parser_entry.get("payload")
            entry_metadata = parser_entry.get("metadata") or {}
            sub_metadata = {**shared_metadata, **entry_metadata}

            canonical = adapter.transform(
                entry_payload,
                document_id=document_id,
                source_uri=source_uri,
                checksum=checksum,
                metadata=sub_metadata,
            )

            combined_text_spans.extend(canonical.text_spans)
            combined_tables.extend(canonical.tables)
            combined_fields.extend(canonical.fields)
            combined_visuals.extend(canonical.visual_descriptions)
            combined_segments.extend(canonical.page_segments)
            combined_attachments.extend(canonical.attachments)
            combined_summaries.extend(canonical.summaries)

            if document_type is None and canonical.document_type is not None:
                document_type = canonical.document_type
            if mime_type is None and canonical.mime_type is not None:
                mime_type = canonical.mime_type

            provider = canonical.metadata.get("provider") if isinstance(canonical.metadata, dict) else None
            parsers_used.append(provider or name)

        combined_attachments.extend(self._parse_additional_attachments(payload_dict.get("attachments")))

        metadata_payload: Dict[str, Any] = {"provider": "multi_parser", **shared_metadata}
        if parsers_used:
            metadata_payload["parsers_used"] = parsers_used

        return CanonicalDocument(
            document_id=document_id,
            source_uri=source_uri,
            checksum=checksum,
            text_spans=combined_text_spans,
            tables=combined_tables,
            fields=combined_fields,
            visual_descriptions=combined_visuals,
            page_segments=combined_segments,
            attachments=combined_attachments,
            summaries=combined_summaries,
            document_type=document_type,
            mime_type=mime_type,
            metadata=metadata_payload,
        )

    @staticmethod
    def _ensure_mapping(payload: Any) -> Dict[str, Any]:
        if payload is None:
            raise AdapterError("MultiParser payload is empty")
        if not isinstance(payload, dict):
            raise AdapterError("MultiParser payload must be a mapping")
        return payload

    def _parse_additional_attachments(self, attachments: Any) -> List[DocumentAttachment]:
        if not attachments:
            return []
        results: List[DocumentAttachment] = []
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            attachment_id = attachment.get("attachment_id") or attachment.get("id")
            file_name = attachment.get("file_name") or attachment.get("name")
            mime_type = attachment.get("mime_type") or attachment.get("content_type")
            if not attachment_id or not file_name or not mime_type:
                continue
            checksum = attachment.get("checksum")
            source_uri = attachment.get("source_uri")
            metadata = attachment.get("metadata") or {}
            canonical_document = attachment.get("canonical_document")
            if canonical_document is not None and not isinstance(canonical_document, CanonicalDocument):
                metadata = {**metadata, "canonical_document": canonical_document}
                canonical_document = None
            results.append(
                DocumentAttachment(
                    attachment_id=str(attachment_id),
                    file_name=str(file_name),
                    mime_type=str(mime_type),
                    checksum=checksum,
                    source_uri=source_uri,
                    document=canonical_document,
                    metadata=metadata,
                )
            )
        return results
