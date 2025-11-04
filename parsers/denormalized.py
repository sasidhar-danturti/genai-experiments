"""Utilities to convert canonical documents into denormalised Pydantic-like rows."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, fields
from datetime import datetime
from enum import Enum
from typing import Dict, Iterable, List, Optional, Tuple, Union

try:  # pragma: no cover - allow either pydantic v1 or v2
    from pydantic import BaseModel, ConfigDict
except ImportError:  # pragma: no cover
    from pydantic import BaseModel  # type: ignore

    ConfigDict = None  # type: ignore

from .canonical_schema import (
    BoundingRegion,
    CanonicalDocument,
    CanonicalTable,
    CanonicalTableCell,
    CanonicalTextSpan,
    DocumentAttachment,
    DocumentEnrichment,
    DocumentSummary,
    StructuredField,
    VisualDescription,
)


class Units(str, Enum):
    PT = "pt"
    PX = "px"
    IN = "in"
    MM = "mm"


class BlockType(str, Enum):
    PARAGRAPH = "paragraph"
    HEADING = "heading"
    LIST_ITEM = "list_item"
    TABLE = "table"
    FIGURE = "figure"
    KV_PAIR = "kv_pair"
    FOOTER = "footer"
    HEADER = "header"


class Status(str, Enum):
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


class InsightKind(str, Enum):
    CLASSIFICATION = "classification"
    EXTRACTION = "extraction"
    SUMMARY = "summary"
    QA = "qa"


class BaseRow(BaseModel):
    schema_version: str = "1.0.0"
    request_id: str = ""
    bundle_id: str = ""
    part_id: Optional[str] = None
    page_number: Optional[int] = None
    generated_at: Optional[datetime] = None

    if 'ConfigDict' in globals() and ConfigDict is not None:  # pragma: no branch
        model_config = ConfigDict(use_enum_values=True)
    else:  # pragma: no cover
        class Config:
            use_enum_values = True

    def to_dict(self) -> Dict[str, object]:
        dump_args = {"exclude_none": True}
        if hasattr(self, "model_dump"):
            payload = self.model_dump(**dump_args)
        else:  # pragma: no cover
            payload = self.dict(**dump_args)
        return payload


class DocRow(BaseRow):
    record_level: str = "doc"
    doc_title: Optional[str] = None
    doc_type: Optional[str] = None
    doc_language: Optional[str] = None
    doc_summary: Optional[str] = None
    total_pages: Optional[int] = None
    total_blocks: Optional[int] = None
    status: Optional[Status] = Status.SUCCEEDED
    insight_label: Optional[str] = None
    insight_conf: Optional[float] = None


class PageRow(BaseRow):
    record_level: str = "page"
    page_number: int = 1
    width: float = 0.0
    height: float = 0.0
    units: Units = Units.PT
    page_text: Optional[str] = None
    page_block_count: Optional[int] = None
    source_mime: Optional[str] = None
    engine_id: Optional[str] = None
    lang_detected: Optional[str] = None


class BlockRow(BaseRow):
    record_level: str = "block"
    page_number: int = 1
    block_id: str = ""
    block_type: BlockType = BlockType.PARAGRAPH
    text: str = ""
    bbox_x: float = 0.0
    bbox_y: float = 0.0
    bbox_w: float = 0.0
    bbox_h: float = 0.0
    confidence: Optional[float] = None
    lines: Optional[List[str]] = None
    table_rows: Optional[int] = None
    table_cols: Optional[int] = None
    kv_key: Optional[str] = None
    kv_value: Optional[str] = None


class InsightRow(BaseRow):
    record_level: str = "insight"
    insight_kind: InsightKind = InsightKind.EXTRACTION
    insight_use_case: Optional[str] = None
    class_label: Optional[str] = None
    class_conf: Optional[float] = None
    field: Optional[str] = None
    value_text: Optional[str] = None
    value_number: Optional[float] = None
    value_unit: Optional[str] = None
    summary_text: Optional[str] = None
    qa_question: Optional[str] = None
    qa_answer: Optional[str] = None
    evidence_part_id: Optional[str] = None
    evidence_page_number: Optional[int] = None
    evidence_block_id: Optional[str] = None


DenormRecord = Union[DocRow, PageRow, BlockRow, InsightRow]


def canonical_to_denorm_records(
    document: CanonicalDocument,
    *,
    request_id: Optional[str] = None,
    generated_at: Optional[datetime] = None,
) -> List[DenormRecord]:
    """Flatten a canonical document (and attachments) into denormalised rows."""

    builder = _DenormBuilder(
        request_id=request_id or document.document_id,
        bundle_id=document.document_id,
        generated_at=generated_at or datetime.utcnow(),
    )
    builder.add_document(document, part_id=None, attachment_label=None)
    return builder.records


class _DenormBuilder:
    def __init__(self, *, request_id: str, bundle_id: str, generated_at: datetime) -> None:
        self.request_id = request_id
        self.bundle_id = bundle_id
        self.generated_at = generated_at
        self.records: List[DenormRecord] = []

    def add_document(
        self,
        document: CanonicalDocument,
        *,
        part_id: Optional[str],
        attachment_label: Optional[str],
    ) -> None:
        block_rows: List[BlockRow] = []
        insight_rows: List[InsightRow] = []
        page_info: Dict[int, Dict[str, object]] = {}

        doc_title = self._resolve_title(document, attachment_label)
        doc_summary = document.summaries[0].summary if document.summaries else None
        doc_language = self._resolve_language(document)
        engine_id = str(document.metadata.get("provider") or "unknown")

        for index, span in enumerate(document.text_spans):
            page_number = self._resolve_page(span.region)
            block_id = span.span_id or self._block_id(part_id, "span", index)
            bbox = _rect_from_region(span.region)
            lines = [line for line in span.content.splitlines() if line.strip()]
            block_rows.append(
                BlockRow(
                    request_id=self.request_id,
                    bundle_id=self.bundle_id,
                    part_id=part_id,
                    generated_at=self.generated_at,
                    page_number=page_number,
                    block_id=block_id,
                    block_type=BlockType.PARAGRAPH,
                    text=span.content,
                    bbox_x=bbox[0],
                    bbox_y=bbox[1],
                    bbox_w=bbox[2],
                    bbox_h=bbox[3],
                    confidence=span.confidence,
                    lines=lines or None,
                )
            )
            self._update_page_info(page_info, page_number, span.content, bbox)

        for index, table in enumerate(document.tables):
            page_number = self._resolve_table_page(table)
            block_id = table.table_id or self._block_id(part_id, "table", index)
            bbox = _table_rect(table)
            text_lines = _table_lines(table)
            block_rows.append(
                BlockRow(
                    request_id=self.request_id,
                    bundle_id=self.bundle_id,
                    part_id=part_id,
                    generated_at=self.generated_at,
                    page_number=page_number,
                    block_id=block_id,
                    block_type=BlockType.TABLE,
                    text="\n".join(text_lines),
                    bbox_x=bbox[0],
                    bbox_y=bbox[1],
                    bbox_w=bbox[2],
                    bbox_h=bbox[3],
                    confidence=table.confidence,
                    lines=text_lines or None,
                    table_rows=_table_rows(table),
                    table_cols=_table_cols(table),
                )
            )
            self._update_page_info(page_info, page_number, " ".join(text_lines), bbox)

        for index, field in enumerate(document.fields):
            page_number = self._resolve_page(field.region)
            block_id = self._block_id(part_id, "field", index)
            bbox = _rect_from_region(field.region)
            text = f"{field.name}: {field.value or ''}".strip()
            block_rows.append(
                BlockRow(
                    request_id=self.request_id,
                    bundle_id=self.bundle_id,
                    part_id=part_id,
                    generated_at=self.generated_at,
                    page_number=page_number,
                    block_id=block_id,
                    block_type=BlockType.KV_PAIR,
                    text=text,
                    bbox_x=bbox[0],
                    bbox_y=bbox[1],
                    bbox_w=bbox[2],
                    bbox_h=bbox[3],
                    confidence=field.confidence,
                    kv_key=field.name,
                    kv_value=field.value,
                )
            )
            self._update_page_info(page_info, page_number, text, bbox)
            insight_rows.append(
                InsightRow(
                    request_id=self.request_id,
                    bundle_id=self.bundle_id,
                    part_id=part_id,
                    generated_at=self.generated_at,
                    insight_kind=InsightKind.EXTRACTION,
                    insight_use_case=field.value_type,
                    field=field.name,
                    value_text=field.value,
                    value_number=_coerce_float(field.value),
                    value_unit=field.value_type,
                    evidence_block_id=block_id,
                    evidence_page_number=page_number,
                    evidence_part_id=part_id,
                )
            )

        for index, visual in enumerate(document.visual_descriptions):
            page_number = self._resolve_page(visual.region)
            block_id = self._block_id(part_id, "figure", index)
            bbox = _rect_from_region(visual.region)
            block_rows.append(
                BlockRow(
                    request_id=self.request_id,
                    bundle_id=self.bundle_id,
                    part_id=part_id,
                    generated_at=self.generated_at,
                    page_number=page_number,
                    block_id=block_id,
                    block_type=BlockType.FIGURE,
                    text=visual.description,
                    bbox_x=bbox[0],
                    bbox_y=bbox[1],
                    bbox_w=bbox[2],
                    bbox_h=bbox[3],
                    confidence=visual.confidence,
                    lines=visual.tags,
                )
            )
            self._update_page_info(page_info, page_number, visual.description, bbox)

        for summary in document.summaries:
            insight_rows.append(
                InsightRow(
                    request_id=self.request_id,
                    bundle_id=self.bundle_id,
                    part_id=part_id,
                    generated_at=self.generated_at,
                    insight_kind=InsightKind.SUMMARY,
                    insight_use_case=summary.method,
                    summary_text=summary.summary,
                    class_conf=summary.confidence,
                )
            )

        for enrichment in document.enrichments:
            insight_rows.append(
                InsightRow(
                    request_id=self.request_id,
                    bundle_id=self.bundle_id,
                    part_id=part_id,
                    generated_at=self.generated_at,
                    insight_kind=_enrichment_kind(enrichment),
                    insight_use_case=enrichment.enrichment_type,
                    value_text=json.dumps(enrichment.content),
                    class_conf=enrichment.confidence,
                )
            )

        if not page_info and document.page_segments:
            for segment in document.page_segments:
                page_info.setdefault(
                    segment.page_number,
                    {"texts": [], "block_count": 0, "width": 0.0, "height": 0.0},
                )

        total_pages = len(page_info) if page_info else None
        page_rows = [
            PageRow(
                request_id=self.request_id,
                bundle_id=self.bundle_id,
                part_id=part_id,
                generated_at=self.generated_at,
                page_number=page_number,
                width=info.get("width", 0.0),
                height=info.get("height", 0.0),
                units=Units.PT,
                page_text=_concatenate_text(info.get("texts")),
                page_block_count=info.get("block_count"),
                source_mime=document.mime_type,
                engine_id=engine_id,
                lang_detected=doc_language,
            )
            for page_number, info in sorted(page_info.items())
        ]

        doc_row = DocRow(
            request_id=self.request_id,
            bundle_id=self.bundle_id,
            part_id=part_id,
            generated_at=self.generated_at,
            doc_title=doc_title,
            doc_type=document.document_type or document.metadata.get("document_type"),
            doc_language=doc_language,
            doc_summary=doc_summary,
            total_pages=total_pages,
            total_blocks=len(block_rows),
            status=Status.SUCCEEDED,
            insight_label=document.metadata.get("classification_label"),
            insight_conf=_coerce_float(document.metadata.get("classification_confidence")),
        )

        self.records.append(doc_row)
        self.records.extend(page_rows)
        self.records.extend(block_rows)
        self.records.extend(insight_rows)

        for attachment_index, attachment in enumerate(document.attachments):
            child_part_id = self._child_part_id(part_id, attachment_index)
            if attachment.document is None:
                self.records.append(
                    DocRow(
                        request_id=self.request_id,
                        bundle_id=self.bundle_id,
                        part_id=child_part_id,
                        generated_at=self.generated_at,
                        doc_title=attachment.file_name,
                        doc_type=attachment.mime_type,
                        status=Status.PARTIAL,
                    )
                )
                continue

            self.add_document(
                attachment.document,
                part_id=child_part_id,
                attachment_label=attachment.file_name,
            )

    def _resolve_title(self, document: CanonicalDocument, attachment_label: Optional[str]) -> Optional[str]:
        if document.summaries and document.summaries[0].title:
            return document.summaries[0].title
        title = document.metadata.get("title") if isinstance(document.metadata, dict) else None
        if title:
            return str(title)
        return attachment_label

    def _resolve_language(self, document: CanonicalDocument) -> Optional[str]:
        for key in ("language", "lang", "doc_language"):
            value = document.metadata.get(key) if isinstance(document.metadata, dict) else None
            if value:
                return str(value)
        return None

    def _resolve_page(self, region: Optional[BoundingRegion]) -> int:
        return region.page if region and region.page else 1

    def _resolve_table_page(self, table: CanonicalTable) -> int:
        for cell in table.cells:
            if cell.region and cell.region.page:
                return cell.region.page
        return 1

    def _block_id(self, part_id: Optional[str], kind: str, index: int) -> str:
        prefix = part_id or "root"
        return f"{prefix}-{kind}-{index + 1}"

    def _child_part_id(self, part_id: Optional[str], index: int) -> str:
        prefix = part_id or "root"
        return f"{prefix}.attachment-{index + 1}"

    def _update_page_info(
        self,
        page_info: Dict[int, Dict[str, object]],
        page_number: int,
        text: str,
        bbox: Tuple[float, float, float, float],
    ) -> None:
        info = page_info.setdefault(
            page_number,
            {"texts": [], "block_count": 0, "width": 0.0, "height": 0.0},
        )
        if text:
            info["texts"].append(text)
        info["block_count"] = int(info.get("block_count", 0)) + 1
        width = bbox[0] + bbox[2]
        height = bbox[1] + bbox[3]
        if width > info.get("width", 0.0):
            info["width"] = width
        if height > info.get("height", 0.0):
            info["height"] = height


def _rect_from_region(region: Optional[BoundingRegion]) -> Tuple[float, float, float, float]:
    if region is None:
        return (0.0, 0.0, 0.0, 0.0)
    if region.bounding_box:
        return _rect_from_sequence(region.bounding_box)
    if region.polygon:
        return _rect_from_sequence(region.polygon)
    return (0.0, 0.0, 0.0, 0.0)


def _rect_from_sequence(sequence: Iterable[float]) -> Tuple[float, float, float, float]:
    coords = list(sequence)
    if not coords:
        return (0.0, 0.0, 0.0, 0.0)
    xs = coords[0::2]
    ys = coords[1::2]
    min_x = float(min(xs))
    min_y = float(min(ys))
    max_x = float(max(xs))
    max_y = float(max(ys))
    return (min_x, min_y, max_x - min_x, max_y - min_y)


def _table_rows(table: CanonicalTable) -> int:
    return max((cell.row_index + cell.row_span for cell in table.cells), default=0)


def _table_cols(table: CanonicalTable) -> int:
    return max((cell.column_index + cell.column_span for cell in table.cells), default=0)


def _table_rect(table: CanonicalTable) -> Tuple[float, float, float, float]:
    boxes = [_rect_from_region(cell.region) for cell in table.cells]
    if not boxes:
        return (0.0, 0.0, 0.0, 0.0)
    min_x = min(box[0] for box in boxes)
    min_y = min(box[1] for box in boxes)
    max_x = max(box[0] + box[2] for box in boxes)
    max_y = max(box[1] + box[3] for box in boxes)
    return (min_x, min_y, max_x - min_x, max_y - min_y)


def _table_lines(table: CanonicalTable) -> List[str]:
    if not table.cells:
        return []
    rows: Dict[int, Dict[int, CanonicalTableCell]] = {}
    for cell in table.cells:
        rows.setdefault(cell.row_index, {})[cell.column_index] = cell
    result: List[str] = []
    for row_index in sorted(rows):
        row_cells = rows[row_index]
        ordered = [row_cells[idx].content for idx in sorted(row_cells)]
        result.append(" | ".join(filter(None, ordered)))
    return result


def _enrichment_kind(enrichment: DocumentEnrichment) -> InsightKind:
    label = enrichment.enrichment_type.lower()
    if "class" in label:
        return InsightKind.CLASSIFICATION
    if label in {"summary", "summaries"}:
        return InsightKind.SUMMARY
    return InsightKind.EXTRACTION


def _coerce_float(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _concatenate_text(value: Optional[object]) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, list):
        parts = [str(part).strip() for part in value if str(part).strip()]
        return " ".join(parts) if parts else None
    return str(value)


__all__ = [
    "BaseRow",
    "DocRow",
    "PageRow",
    "BlockRow",
    "InsightRow",
    "Units",
    "BlockType",
    "Status",
    "InsightKind",
    "DenormRecord",
    "canonical_to_denorm_records",
]
