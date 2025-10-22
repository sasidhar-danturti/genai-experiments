"""Adapter for PyMuPDF extraction results."""

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


class PyMuPDFAdapter(ParserAdapter):
    """Normalise PyMuPDF structured extraction payloads into the canonical schema."""

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
        pages = payload_dict.get("pages")
        if not isinstance(pages, list) or not pages:
            raise AdapterError("PyMuPDF payload must include a non-empty 'pages' list")

        text_spans: List[CanonicalTextSpan] = []
        tables: List[CanonicalTable] = []
        fields: List[StructuredField] = []
        page_segments: List[PageSegment] = []

        for page in pages:
            if not isinstance(page, dict):
                continue
            page_number = int(page.get("page_number") or page.get("number") or page.get("index") or 1)
            page_confidence = self._normalise_confidence(page.get("confidence"))
            page_segments.append(
                PageSegment(
                    page_number=page_number,
                    parser="pymupdf",
                    method=page.get("method") or "text",
                    confidence=page_confidence,
                    metadata={"rotation": page.get("rotation")} if page.get("rotation") is not None else {},
                )
            )

            text_spans.extend(self._parse_page_text(page, page_number))
            tables.extend(self._parse_page_tables(page, page_number))
            fields.extend(self._parse_page_fields(page, page_number))

        # Include global fields if present
        fields.extend(self._parse_structured_fields(payload_dict.get("fields"), page_hint=None))

        metadata_payload = {"provider": "pymupdf", **(payload_dict.get("metadata") or {}), **(metadata or {})}
        document_type = metadata_payload.get("document_type") or payload_dict.get("document_type") or "document"
        mime_type = metadata_payload.get("mime_type") or payload_dict.get("mime_type")

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
    # Page parsing helpers
    # ------------------------------------------------------------------

    def _parse_page_text(self, page: Dict[str, Any], page_number: int) -> Iterable[CanonicalTextSpan]:
        text_items = self._collect_text_items(page)
        for idx, item in enumerate(text_items):
            if not isinstance(item, dict):
                continue
            content = item.get("content") or item.get("text")
            if not content:
                continue
            confidence = self._normalise_confidence(item.get("confidence") or page.get("confidence"))
            span_id = item.get("id") or f"p{page_number}-span-{idx}"
            region = self._build_region(item, page_number)
            provenance = ExtractionProvenance(
                parser="pymupdf",
                method=item.get("method") or "text_block",
                page_span=[region.page] if region else [page_number],
            )
            confidence_signals = [
                ConfidenceSignal(
                    source="pymupdf",
                    confidence=confidence,
                    method=provenance.method,
                )
            ]
            yield CanonicalTextSpan(
                content=str(content),
                confidence=confidence,
                region=region,
                span_id=str(span_id),
                provenance=provenance,
                confidence_signals=confidence_signals,
            )

    def _parse_page_tables(self, page: Dict[str, Any], page_number: int) -> Iterable[CanonicalTable]:
        tables = page.get("tables") or []
        for table_index, table in enumerate(tables):
            if not isinstance(table, dict):
                continue
            table_id = table.get("id") or f"p{page_number}-table-{table_index}"
            table_confidence = self._normalise_confidence(table.get("confidence") or page.get("confidence"))
            cells: List[CanonicalTableCell] = []
            for cell_index, cell in enumerate(table.get("cells") or []):
                if not isinstance(cell, dict):
                    continue
                content = cell.get("content") or cell.get("text") or ""
                row_index = int(cell.get("row_index") or cell.get("row") or 0)
                column_index = int(cell.get("column_index") or cell.get("column") or 0)
                row_span = int(cell.get("row_span") or cell.get("rowSpan") or 1)
                column_span = int(cell.get("column_span") or cell.get("col_span") or cell.get("columnSpan") or 1)
                confidence = self._normalise_confidence(cell.get("confidence") or table.get("confidence"))
                region = self._build_region(cell, page_number)
                provenance = ExtractionProvenance(
                    parser="pymupdf",
                    method="table_cell",
                    page_span=[region.page] if region else [page_number],
                )
                confidence_signals = [
                    ConfidenceSignal(
                        source="pymupdf",
                        confidence=confidence,
                        method="table_cell",
                    )
                ]
                cells.append(
                    CanonicalTableCell(
                        row_index=row_index,
                        column_index=column_index,
                        content=str(content),
                        confidence=confidence,
                        region=region,
                        row_span=row_span,
                        column_span=column_span,
                        provenance=provenance,
                        confidence_signals=confidence_signals,
                    )
                )

            provenance = ExtractionProvenance(
                parser="pymupdf",
                method="table",
                page_span=[page_number],
            )

            yield CanonicalTable(
                table_id=str(table_id),
                confidence=table_confidence,
                cells=cells,
                caption=table.get("caption"),
                footnotes=list(table.get("footnotes") or []),
                provenance=provenance,
            )

    def _parse_page_fields(self, page: Dict[str, Any], page_number: int) -> Iterable[StructuredField]:
        yield from self._parse_structured_fields(page.get("fields"), page_hint=page_number)

    # ------------------------------------------------------------------
    # Structured field helpers
    # ------------------------------------------------------------------

    def _parse_structured_fields(self, fields: Any, page_hint: Optional[int]) -> Iterable[StructuredField]:
        if not fields:
            return []
        items: Iterable[Any]
        if isinstance(fields, dict):
            items = fields.items()
        else:
            items = enumerate(fields)
        results: List[StructuredField] = []
        for name, payload in items:
            if not isinstance(payload, dict):
                continue
            value = payload.get("value") or payload.get("text")
            confidence = self._normalise_confidence(payload.get("confidence"))
            region = self._build_region(payload, page_hint)
            provenance = ExtractionProvenance(
                parser="pymupdf",
                method=payload.get("method") or "field",
                page_span=[region.page] if region else ([page_hint] if page_hint is not None else None),
            )
            confidence_signals = [
                ConfidenceSignal(
                    source="pymupdf",
                    confidence=confidence,
                    method=provenance.method,
                )
            ]
            results.append(
                StructuredField(
                    name=str(name),
                    value=None if value is None else str(value),
                    confidence=confidence,
                    value_type=payload.get("value_type") or payload.get("type"),
                    region=region,
                    provenance=provenance,
                    confidence_signals=confidence_signals,
                )
            )
        return results

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _ensure_mapping(payload: Any) -> Dict[str, Any]:
        if payload is None:
            raise AdapterError("PyMuPDF payload is empty")
        if not isinstance(payload, dict):
            raise AdapterError("PyMuPDF payload must be a mapping")
        return payload

    def _collect_text_items(self, page: Dict[str, Any]) -> List[Any]:
        for key in ("text_spans", "spans", "text_blocks", "blocks", "lines"):
            if isinstance(page.get(key), list):
                return list(page[key])
        text = page.get("text")
        if isinstance(text, str) and text.strip():
            return [{"content": text, "confidence": page.get("confidence")}]
        return []

    def _build_region(self, payload: Optional[Dict[str, Any]], default_page: Optional[int]) -> BoundingRegion:
        if payload is None:
            page = default_page or 1
            return BoundingRegion(page=int(page))
        page_value = payload.get("page") or payload.get("page_number") or payload.get("pageNumber")
        page = int(page_value) if page_value is not None else int(default_page or 1)
        polygon = payload.get("polygon")
        bounding_box = payload.get("bounding_box") or payload.get("bbox") or payload.get("rect")
        return BoundingRegion(
            page=page,
            polygon=_ensure_list_of_float(polygon),
            bounding_box=_ensure_list_of_float(bounding_box),
        )


def _ensure_list_of_float(values: Any) -> Optional[List[float]]:
    if values is None:
        return None
    return [float(value) for value in values]
