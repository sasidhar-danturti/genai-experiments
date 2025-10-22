"""Parser utilities for normalising document intelligence responses."""

from .canonical_schema import (
    BoundingRegion,
    CanonicalDocument,
    CanonicalTable,
    CanonicalTableCell,
    CanonicalTextSpan,
    ConfidenceSignal,
    DocumentAttachment,
    ExtractionProvenance,
    PageSegment,
    StructuredField,
    VisualDescription,
)

__all__ = [
    "BoundingRegion",
    "CanonicalDocument",
    "CanonicalTable",
    "CanonicalTableCell",
    "CanonicalTextSpan",
    "ConfidenceSignal",
    "DocumentAttachment",
    "ExtractionProvenance",
    "PageSegment",
    "StructuredField",
    "VisualDescription",
]
