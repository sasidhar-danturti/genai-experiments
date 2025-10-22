"""Adapter that normalises Databricks-hosted LLM image parsing responses."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

from ..canonical_schema import (
    BoundingRegion,
    CanonicalDocument,
    CanonicalTextSpan,
    ConfidenceSignal,
    ExtractionProvenance,
    PageSegment,
    StructuredField,
    VisualDescription,
)
from .base import AdapterError, ParserAdapter


class DatabricksLLMImageAdapter(ParserAdapter):
    """Normalise Databricks LLM image parsing responses into the canonical schema."""

    def transform(
        self,
        payload: Any,
        *,
        document_id: str,
        source_uri: str,
        checksum: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CanonicalDocument:
        payload_dict = self._coerce_payload(payload)
        text_spans = list(self._parse_text(payload_dict))
        fields = list(self._parse_fields(payload_dict))
        visuals = list(self._parse_visuals(payload_dict))
        page_segments = list(self._derive_page_segments(text_spans, payload_dict))

        metadata_payload = {"provider": "databricks_llm_image", **(metadata or {})}
        overall_description = payload_dict.get("overall_description") or payload_dict.get("summary")
        if overall_description and "overall_description" not in metadata_payload:
            metadata_payload["overall_description"] = overall_description

        document_type = metadata_payload.get("document_type") or "image"
        mime_type = metadata_payload.get("mime_type") or metadata_payload.get("content_type") or "image"

        return CanonicalDocument(
            document_id=document_id,
            source_uri=source_uri,
            checksum=checksum,
            text_spans=text_spans,
            tables=[],
            fields=fields,
            visual_descriptions=visuals,
            page_segments=page_segments,
            document_type=document_type,
            mime_type=mime_type,
            metadata=metadata_payload,
        )

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_text(self, payload: Dict[str, Any]) -> Iterable[CanonicalTextSpan]:
        spans = self._get_collection(payload, "text_spans") or self._get_collection(payload, "textSegments") or []
        for idx, span in enumerate(spans):
            if not isinstance(span, dict):
                continue
            content = span.get("content") or span.get("text")
            if not content:
                continue
            confidence = self._normalise_confidence(span.get("confidence"))
            span_id = span.get("id") or span.get("span_id") or f"span-{idx}"
            region = self._build_region(span)
            provenance = ExtractionProvenance(
                parser="databricks_llm_image",
                method="llm_text",
                page_span=[region.page] if region else None,
            )
            confidence_signals = [
                ConfidenceSignal(
                    source="databricks_llm_image",
                    confidence=confidence,
                    method="llm_text",
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

    def _parse_fields(self, payload: Dict[str, Any]) -> Iterable[StructuredField]:
        fields = self._get_collection(payload, "fields") or []
        for entry in fields:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            if not name:
                continue
            value = entry.get("value")
            confidence = self._normalise_confidence(entry.get("confidence"))
            value_type = entry.get("value_type") or entry.get("type")
            region = self._build_region(entry)
            provenance = ExtractionProvenance(
                parser="databricks_llm_image",
                method="llm_field",
                page_span=[region.page] if region else None,
            )
            confidence_signals = [
                ConfidenceSignal(
                    source="databricks_llm_image",
                    confidence=confidence,
                    method="llm_field",
                )
            ]
            yield StructuredField(
                name=str(name),
                value=None if value is None else str(value),
                confidence=confidence,
                value_type=value_type,
                region=region,
                provenance=provenance,
                confidence_signals=confidence_signals,
            )

    def _parse_visuals(self, payload: Dict[str, Any]) -> Iterable[VisualDescription]:
        visuals = self._get_collection(payload, "visual_descriptions") or self._get_collection(payload, "visualDescriptions") or []
        for idx, entry in enumerate(visuals):
            if not isinstance(entry, dict):
                continue
            description = entry.get("description") or entry.get("content")
            if not description:
                continue
            confidence = self._normalise_confidence(entry.get("confidence"))
            tags = entry.get("tags") or entry.get("labels")
            if tags is not None:
                tags = list(tags)
            region = self._build_region(entry)
            provenance = ExtractionProvenance(
                parser="databricks_llm_image",
                method="vision_description",
                page_span=[region.page] if region else None,
            )
            confidence_signals = [
                ConfidenceSignal(
                    source="databricks_llm_image",
                    confidence=confidence,
                    method="vision_description",
                )
            ]
            yield VisualDescription(
                description=description,
                confidence=confidence,
                region=region,
                tags=tags,
                provenance=provenance,
                confidence_signals=confidence_signals,
            )

        if not visuals:
            # If only a single high-level description exists, surface it as a visual description
            overall = payload.get("overall_description") or payload.get("summary")
            if overall:
                provenance = ExtractionProvenance(
                    parser="databricks_llm_image",
                    method="vision_description",
                )
                confidence_signal = ConfidenceSignal(
                    source="databricks_llm_image",
                    confidence=1.0,
                    method="vision_description",
                )
                yield VisualDescription(
                    description=str(overall),
                    confidence=1.0,
                    provenance=provenance,
                    confidence_signals=[confidence_signal],
                )

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_payload(payload: Any) -> Dict[str, Any]:
        if payload is None:
            raise AdapterError("Databricks LLM payload is empty")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError as exc:  # pragma: no cover - defensive
                raise AdapterError("Databricks LLM payload must be JSON serialisable") from exc
        if not isinstance(payload, dict):
            raise AdapterError("Databricks LLM payload must be a mapping")
        return payload

    @staticmethod
    def _get_collection(payload: Dict[str, Any], key: str) -> Optional[Iterable[Any]]:
        value = payload.get(key)
        if value is not None:
            return value
        camel_key = _snake_to_camel(key)
        if camel_key in payload:
            return payload[camel_key]
        return None

    def _build_region(self, entry: Dict[str, Any]) -> Optional[BoundingRegion]:
        page = entry.get("page") or entry.get("page_number") or entry.get("pageNumber")
        polygon = entry.get("polygon")
        bounding_box = entry.get("bounding_box") or entry.get("boundingBox")
        if page is None and polygon is None and bounding_box is None:
            return None
        if page is None:
            page = 1
        return BoundingRegion(
            page=int(page),
            polygon=_ensure_list_of_float(polygon),
            bounding_box=_ensure_list_of_float(bounding_box),
        )

    def _derive_page_segments(
        self, text_spans: Iterable[CanonicalTextSpan], payload: Dict[str, Any]
    ) -> Iterable[PageSegment]:
        seen: Dict[int, PageSegment] = {}
        for span in text_spans:
            if span.region is None:
                continue
            page_number = span.region.page
            if page_number not in seen:
                seen[page_number] = PageSegment(
                    page_number=page_number,
                    parser="databricks_llm_image",
                    method="vision",
                )
        if not seen:
            # If no explicit regions were returned treat the payload as a single-page image
            default_page = int(payload.get("page") or 1)
            seen[default_page] = PageSegment(
                page_number=default_page,
                parser="databricks_llm_image",
                method="vision",
            )
        return seen.values()


def _snake_to_camel(name: str) -> str:
    parts = name.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:]) if parts else name


def _ensure_list_of_float(values: Any) -> Optional[List[float]]:
    if values is None:
        return None
    return [float(value) for value in values]
