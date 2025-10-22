"""Canonical schema definitions for document intelligence outputs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

SCHEMA_VERSION = "1.1"


@dataclass(frozen=True)
class BoundingRegion:
    """Represents the physical location of an element on a page."""

    page: int
    polygon: Optional[List[float]] = None
    bounding_box: Optional[List[float]] = None

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {"page": self.page}
        if self.polygon is not None:
            payload["polygon"] = list(self.polygon)
        if self.bounding_box is not None:
            payload["bounding_box"] = list(self.bounding_box)
        return payload


@dataclass(frozen=True)
class ConfidenceSignal:
    """Represents a single confidence contribution from a parser."""

    source: str
    confidence: float
    method: Optional[str] = None
    model: Optional[str] = None
    weight: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "source": self.source,
            "confidence": self.confidence,
        }
        if self.method is not None:
            payload["method"] = self.method
        if self.model is not None:
            payload["model"] = self.model
        if self.weight is not None:
            payload["weight"] = self.weight
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        return payload


@dataclass(frozen=True)
class ExtractionProvenance:
    """Describes how a canonical element was extracted."""

    parser: str
    method: Optional[str] = None
    model: Optional[str] = None
    source: Optional[str] = None
    page_span: Optional[List[int]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "parser": self.parser,
        }
        if self.method is not None:
            payload["method"] = self.method
        if self.model is not None:
            payload["model"] = self.model
        if self.source is not None:
            payload["source"] = self.source
        if self.page_span is not None:
            payload["page_span"] = list(self.page_span)
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        return payload


@dataclass(frozen=True)
class CanonicalTextSpan:
    """Normalised representation of a text span."""

    content: str
    confidence: float
    region: Optional[BoundingRegion] = None
    span_id: Optional[str] = None
    provenance: Optional[ExtractionProvenance] = None
    confidence_signals: List[ConfidenceSignal] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        payload = {
            "content": self.content,
            "confidence": self.confidence,
        }
        if self.region is not None:
            payload["region"] = self.region.to_dict()
        if self.span_id is not None:
            payload["span_id"] = self.span_id
        if self.provenance is not None:
            payload["provenance"] = self.provenance.to_dict()
        if self.confidence_signals:
            payload["confidence_signals"] = [signal.to_dict() for signal in self.confidence_signals]
        return payload


@dataclass(frozen=True)
class VisualDescription:
    """Describes visual context for non-textual document elements such as images."""

    description: str
    confidence: float
    region: Optional[BoundingRegion] = None
    tags: Optional[List[str]] = None
    provenance: Optional[ExtractionProvenance] = None
    confidence_signals: List[ConfidenceSignal] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "description": self.description,
            "confidence": self.confidence,
        }
        if self.region is not None:
            payload["region"] = self.region.to_dict()
        if self.tags is not None:
            payload["tags"] = list(self.tags)
        if self.provenance is not None:
            payload["provenance"] = self.provenance.to_dict()
        if self.confidence_signals:
            payload["confidence_signals"] = [signal.to_dict() for signal in self.confidence_signals]
        return payload


@dataclass(frozen=True)
class CanonicalTableCell:
    """Cell within a canonical table."""

    row_index: int
    column_index: int
    content: str
    confidence: float
    region: BoundingRegion
    row_span: int = 1
    column_span: int = 1
    provenance: Optional[ExtractionProvenance] = None
    confidence_signals: List[ConfidenceSignal] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        payload = {
            "row_index": self.row_index,
            "column_index": self.column_index,
            "content": self.content,
            "confidence": self.confidence,
            "region": self.region.to_dict(),
            "row_span": self.row_span,
            "column_span": self.column_span,
        }
        if self.provenance is not None:
            payload["provenance"] = self.provenance.to_dict()
        if self.confidence_signals:
            payload["confidence_signals"] = [signal.to_dict() for signal in self.confidence_signals]
        return payload


@dataclass(frozen=True)
class CanonicalTable:
    """Normalized table representation."""

    table_id: str
    confidence: float
    cells: List[CanonicalTableCell] = field(default_factory=list)
    caption: Optional[str] = None
    footnotes: Optional[List[str]] = None
    provenance: Optional[ExtractionProvenance] = None

    def to_dict(self) -> Dict[str, object]:
        payload = {
            "table_id": self.table_id,
            "confidence": self.confidence,
            "cells": [cell.to_dict() for cell in self.cells],
        }
        if self.caption is not None:
            payload["caption"] = self.caption
        if self.footnotes is not None:
            payload["footnotes"] = list(self.footnotes)
        if self.provenance is not None:
            payload["provenance"] = self.provenance.to_dict()
        return payload


@dataclass(frozen=True)
class StructuredField:
    """Canonical representation of a structured field."""

    name: str
    value: Optional[str]
    confidence: float
    value_type: Optional[str] = None
    region: Optional[BoundingRegion] = None
    provenance: Optional[ExtractionProvenance] = None
    confidence_signals: List[ConfidenceSignal] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "name": self.name,
            "value": self.value,
            "confidence": self.confidence,
        }
        if self.value_type is not None:
            payload["value_type"] = self.value_type
        if self.region is not None:
            payload["region"] = self.region.to_dict()
        if self.provenance is not None:
            payload["provenance"] = self.provenance.to_dict()
        if self.confidence_signals:
            payload["confidence_signals"] = [signal.to_dict() for signal in self.confidence_signals]
        return payload


@dataclass(frozen=True)
class PageSegment:
    """Describes which parser processed a particular page."""

    page_number: int
    parser: str
    method: Optional[str] = None
    confidence: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "page_number": self.page_number,
            "parser": self.parser,
        }
        if self.method is not None:
            payload["method"] = self.method
        if self.confidence is not None:
            payload["confidence"] = self.confidence
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        return payload


@dataclass(frozen=True)
class DocumentAttachment:
    """Represents an attachment belonging to a canonical document (e.g. email)."""

    attachment_id: str
    file_name: str
    mime_type: str
    checksum: Optional[str] = None
    source_uri: Optional[str] = None
    document: Optional["CanonicalDocument"] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "attachment_id": self.attachment_id,
            "file_name": self.file_name,
            "mime_type": self.mime_type,
        }
        if self.checksum is not None:
            payload["checksum"] = self.checksum
        if self.source_uri is not None:
            payload["source_uri"] = self.source_uri
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        if self.document is not None:
            payload["document"] = self.document.to_dict()
        return payload


@dataclass(frozen=True)
class DocumentSummary:
    """Machine- or heuristically-generated summary for a document."""

    summary: str
    confidence: float
    method: str
    title: Optional[str] = None
    model: Optional[str] = None
    justification: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "summary": self.summary,
            "confidence": self.confidence,
            "method": self.method,
        }
        if self.title is not None:
            payload["title"] = self.title
        if self.model is not None:
            payload["model"] = self.model
        if self.justification is not None:
            payload["justification"] = self.justification
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        return payload


@dataclass(frozen=True)
class CanonicalDocument:
    """Top-level canonical document payload."""

    document_id: str
    source_uri: str
    checksum: str
    text_spans: List[CanonicalTextSpan]
    tables: List[CanonicalTable]
    fields: List[StructuredField]
    visual_descriptions: List[VisualDescription] = field(default_factory=list)
    page_segments: List[PageSegment] = field(default_factory=list)
    attachments: List[DocumentAttachment] = field(default_factory=list)
    summaries: List[DocumentSummary] = field(default_factory=list)
    document_type: Optional[str] = None
    mime_type: Optional[str] = None
    schema_version: str = SCHEMA_VERSION
    metadata: Dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        payload = {
            "document_id": self.document_id,
            "source_uri": self.source_uri,
            "checksum": self.checksum,
            "schema_version": self.schema_version,
            "metadata": dict(self.metadata),
            "text_spans": [span.to_dict() for span in self.text_spans],
            "tables": [table.to_dict() for table in self.tables],
            "fields": [field.to_dict() for field in self.fields],
        }
        if self.summaries:
            payload["summaries"] = [summary.to_dict() for summary in self.summaries]
        if self.visual_descriptions:
            payload["visual_descriptions"] = [visual.to_dict() for visual in self.visual_descriptions]
        if self.page_segments:
            payload["page_segments"] = [segment.to_dict() for segment in self.page_segments]
        if self.attachments:
            payload["attachments"] = [attachment.to_dict() for attachment in self.attachments]
        if self.document_type is not None:
            payload["document_type"] = self.document_type
        if self.mime_type is not None:
            payload["mime_type"] = self.mime_type
        return payload

    def to_record(self) -> Dict[str, object]:
        """Return a dictionary suitable for persistence."""

        return self.to_dict()


def flatten_tables(tables: Iterable[CanonicalTable]) -> List[Dict[str, object]]:
    """Utility helper that flattens table cells for tabular storage."""

    flattened: List[Dict[str, object]] = []
    for table in tables:
        for cell in table.cells:
            flattened.append(
                {
                    "table_id": table.table_id,
                    "table_confidence": table.confidence,
                    "cell": cell.to_dict(),
                    "caption": table.caption,
                    "footnotes": table.footnotes,
                }
            )
    return flattened
