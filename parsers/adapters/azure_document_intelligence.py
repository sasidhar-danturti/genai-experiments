"""Adapter for Azure Document Intelligence responses."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from ..canonical_schema import (
    BoundingRegion,
    CanonicalDocument,
    CanonicalTable,
    CanonicalTableCell,
    CanonicalTextSpan,
    ConfidenceSignal,
    ExtractionProvenance,
    PageSegment,
    StructuredField,
)
from .base import AdapterError, ParserAdapter


class AzureDocumentIntelligenceAdapter(ParserAdapter):
    """Transform Azure Document Intelligence SDK payloads to the canonical schema."""

    def transform(
        self,
        payload: Any,
        *,
        document_id: str,
        source_uri: str,
        checksum: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CanonicalDocument:
        analyze_result = self._extract_analyze_result(payload)
        text_spans = list(self._parse_text_spans(analyze_result))
        tables = list(self._parse_tables(analyze_result))
        fields = list(self._parse_fields(analyze_result))
        page_segments = list(self._build_page_segments(analyze_result))
        if not page_segments:
            inferred_pages = {
                span.region.page
                for span in text_spans
                if span.region is not None
            }
            if inferred_pages:
                page_segments = [
                    PageSegment(
                        page_number=page_number,
                        parser="azure_document_intelligence",
                        method="inferred",
                    )
                    for page_number in sorted(inferred_pages)
                ]
            else:
                page_segments = [
                    PageSegment(
                        page_number=1,
                        parser="azure_document_intelligence",
                        method="analysis",
                    )
                ]
        metadata_payload = {"provider": "azure_document_intelligence", **(metadata or {})}
        document_type = metadata_payload.get("document_type")
        mime_type = metadata_payload.get("mime_type") or metadata_payload.get("content_type")

        return CanonicalDocument(
            document_id=document_id,
            source_uri=source_uri,
            checksum=checksum,
            text_spans=text_spans,
            tables=tables,
            fields=fields,
            visual_descriptions=[],
            page_segments=page_segments,
            document_type=document_type,
            mime_type=mime_type,
            metadata=metadata_payload,
        )

    # ------------------------------------------------------------------
    # Text spans
    # ------------------------------------------------------------------

    def _parse_text_spans(self, analyze_result: Any) -> Iterable[CanonicalTextSpan]:
        paragraphs = self._get_collection(analyze_result, "paragraphs")
        for idx, paragraph in enumerate(paragraphs or []):
            content = self._get_attr(paragraph, "content")
            if not content:
                continue
            region = self._first_region(paragraph)
            confidence = self._normalise_confidence(self._get_attr(paragraph, "confidence"))
            provenance = ExtractionProvenance(
                parser="azure_document_intelligence",
                method="paragraph",
                page_span=[region.page] if region else None,
            )
            confidence_signals = [
                ConfidenceSignal(
                    source="azure_document_intelligence",
                    confidence=confidence,
                    method="paragraph",
                )
            ]
            yield CanonicalTextSpan(
                content=content,
                confidence=confidence,
                region=region,
                span_id=self._get_attr(paragraph, "id", default=str(idx)),
                provenance=provenance,
                confidence_signals=confidence_signals,
            )

        if paragraphs:
            return

        # Fallback to page-level lines when paragraphs are not available
        pages = self._get_collection(analyze_result, "pages")
        for page in pages or []:
            page_number = self._get_attr(page, "page_number", default=self._get_attr(page, "pageNumber", default=1))
            lines = self._get_collection(page, "lines")
            for idx, line in enumerate(lines or []):
                content = self._get_attr(line, "content")
                if not content:
                    continue
                region = self._first_region(line, default_page=page_number)
                confidence = self._normalise_confidence(self._get_attr(line, "confidence"))
                span_id = self._get_attr(line, "id", default=f"page-{page_number}-line-{idx}")
                provenance = ExtractionProvenance(
                    parser="azure_document_intelligence",
                    method="line",
                    page_span=[region.page] if region else [page_number],
                )
                confidence_signals = [
                    ConfidenceSignal(
                        source="azure_document_intelligence",
                        confidence=confidence,
                        method="line",
                    )
                ]
                yield CanonicalTextSpan(
                    content=content,
                    confidence=confidence,
                    region=region,
                    span_id=span_id,
                    provenance=provenance,
                    confidence_signals=confidence_signals,
                )

    # ------------------------------------------------------------------
    # Tables
    # ------------------------------------------------------------------

    def _parse_tables(self, analyze_result: Any) -> Iterable[CanonicalTable]:
        tables = self._get_collection(analyze_result, "tables") or []
        for table in tables:
            table_id = self._get_attr(table, "id", default=None)
            confidence = self._normalise_confidence(self._get_attr(table, "confidence"))
            cells = []
            for cell in self._get_collection(table, "cells") or []:
                region = self._first_region(cell)
                cell_confidence = self._normalise_confidence(self._get_attr(cell, "confidence"))
                provenance = ExtractionProvenance(
                    parser="azure_document_intelligence",
                    method="table_cell",
                    page_span=[region.page] if region else None,
                )
                confidence_signals = [
                    ConfidenceSignal(
                        source="azure_document_intelligence",
                        confidence=cell_confidence,
                        method="table_cell",
                    )
                ]
                cells.append(
                    CanonicalTableCell(
                        row_index=self._get_attr(cell, "row_index", default=self._get_attr(cell, "rowIndex", default=0)),
                        column_index=self._get_attr(cell, "column_index", default=self._get_attr(cell, "columnIndex", default=0)),
                        content=self._get_attr(cell, "content", default=""),
                        confidence=cell_confidence,
                        region=region,
                        row_span=self._get_attr(cell, "row_span", default=self._get_attr(cell, "rowSpan", default=1)),
                        column_span=self._get_attr(cell, "column_span", default=self._get_attr(cell, "columnSpan", default=1)),
                        provenance=provenance,
                        confidence_signals=confidence_signals,
                    )
                )

            caption = self._get_attr(table, "caption")
            footnotes = self._get_attr(table, "footnotes")
            if footnotes is not None:
                footnotes = list(footnotes)

            table_provenance = ExtractionProvenance(
                parser="azure_document_intelligence",
                method="table",
            )

            yield CanonicalTable(
                table_id=table_id or "table-{}".format(self._get_attr(table, "index", default="unknown")),
                confidence=confidence,
                cells=cells,
                caption=caption,
                footnotes=footnotes,
                provenance=table_provenance,
            )

    # ------------------------------------------------------------------
    # Fields
    # ------------------------------------------------------------------

    def _parse_fields(self, analyze_result: Any) -> Iterable[StructuredField]:
        documents = self._get_collection(analyze_result, "documents") or []
        for document in documents:
            fields = self._get_attr(document, "fields", default={}) or {}
            if isinstance(fields, dict):
                iterator = fields.items()
            else:
                iterator = enumerate(fields)
            for name, field in iterator:
                if field is None:
                    continue
                value = self._get_attr(field, "value", default=self._get_attr(field, "content"))
                field_type = self._get_attr(field, "type", default=self._get_attr(field, "value_type"))
                confidence = self._normalise_confidence(self._get_attr(field, "confidence"))
                region = None
                region_payload = self._first_region(field, optional=True)
                if region_payload:
                    region = region_payload
                provenance = ExtractionProvenance(
                    parser="azure_document_intelligence",
                    method="field",
                    page_span=[region.page] if region else None,
                )
                confidence_signals = [
                    ConfidenceSignal(
                        source="azure_document_intelligence",
                        confidence=confidence,
                        method="field",
                        model=self._get_attr(field, "model_id", default=None),
                    )
                ]
                yield StructuredField(
                    name=str(name),
                    value=None if value is None else str(value),
                    confidence=confidence,
                    value_type=field_type,
                    region=region,
                    provenance=provenance,
                    confidence_signals=confidence_signals,
                )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_analyze_result(payload: Any) -> Any:
        if payload is None:
            raise AdapterError("Azure Document Intelligence payload is empty")
        if hasattr(payload, "analyze_result") and getattr(payload, "analyze_result") is not None:
            return getattr(payload, "analyze_result")
        if isinstance(payload, dict) and "analyzeResult" in payload:
            return payload["analyzeResult"]
        return payload

    @staticmethod
    def _get_collection(obj: Any, attr: str) -> Optional[Iterable[Any]]:
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(attr) or obj.get(_camel_to_snake(attr))
        value = getattr(obj, attr, None)
        if value is None:
            camel_attr = _snake_to_camel(attr)
            value = getattr(obj, camel_attr, None)
        return value

    @staticmethod
    def _get_attr(obj: Any, attr: str, default=None):
        if obj is None:
            return default
        if isinstance(obj, dict):
            if attr in obj:
                return obj[attr]
            snake_attr = _camel_to_snake(attr)
            if snake_attr in obj:
                return obj[snake_attr]
            camel_attr = _snake_to_camel(attr)
            if camel_attr in obj:
                return obj[camel_attr]
            return default
        if hasattr(obj, attr):
            return getattr(obj, attr)
        camel_attr = _snake_to_camel(attr)
        if hasattr(obj, camel_attr):
            return getattr(obj, camel_attr)
        snake_attr = _camel_to_snake(attr)
        if hasattr(obj, snake_attr):
            return getattr(obj, snake_attr)
        return default

    def _first_region(self, obj: Any, default_page: Optional[int] = None, optional: bool = False) -> BoundingRegion:
        bounding_regions = self._get_attr(obj, "bounding_regions", default=None)
        if not bounding_regions:
            bounding_regions = self._get_attr(obj, "regions", default=None)
        region_payload = None
        if bounding_regions:
            if isinstance(bounding_regions, list):
                region_payload = bounding_regions[0] if bounding_regions else None
            else:
                region_payload = next(iter(bounding_regions), None)
        if region_payload is None:
            if optional:
                return None  # type: ignore[return-value]
            page = default_page if default_page is not None else 1
            return BoundingRegion(page=page)
        page_number = self._get_attr(region_payload, "page_number", default=self._get_attr(region_payload, "pageNumber", default=default_page or 1))
        polygon = self._get_attr(region_payload, "polygon")
        bounding_box = self._get_attr(region_payload, "bounding_box", default=self._get_attr(region_payload, "boundingBox"))
        return BoundingRegion(page=int(page_number), polygon=_ensure_list_of_float(polygon), bounding_box=_ensure_list_of_float(bounding_box))

    def _build_page_segments(self, analyze_result: Any) -> Iterable[PageSegment]:
        pages = self._get_collection(analyze_result, "pages") or []
        for page in pages:
            page_number = self._get_attr(page, "page_number", default=self._get_attr(page, "pageNumber", default=1))
            yield PageSegment(
                page_number=int(page_number),
                parser="azure_document_intelligence",
                method="layout",
                confidence=self._normalise_confidence(self._get_attr(page, "confidence")),
            )


def _snake_to_camel(name: str) -> str:
    parts = name.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:]) if parts else name


def _camel_to_snake(name: str) -> str:
    result = []
    for char in name:
        if char.isupper():
            result.append("_")
            result.append(char.lower())
        else:
            result.append(char)
    snake = "".join(result)
    if snake.startswith("_"):
        snake = snake[1:]
    return snake


def _ensure_list_of_float(values: Any) -> Optional[List[float]]:
    if values is None:
        return None
    return [float(value) for value in values]
