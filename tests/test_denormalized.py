import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from parsers.canonical_schema import (
    BoundingRegion,
    CanonicalDocument,
    CanonicalTable,
    CanonicalTableCell,
    CanonicalTextSpan,
    DocumentAttachment,
    DocumentEnrichment,
    DocumentSummary,
    ExtractionProvenance,
    StructuredField,
)
from parsers.denormalized import (
    BlockRow,
    BlockType,
    DocRow,
    InsightKind,
    InsightRow,
    PageRow,
    Status,
    canonical_to_denorm_records,
)


def test_canonical_to_denorm_records_produces_expected_rows():
    span = CanonicalTextSpan(
        content="Invoice 123",
        confidence=0.9,
        region=BoundingRegion(page=1, bounding_box=[0.0, 0.0, 100.0, 40.0]),
        span_id="span-1",
        provenance=ExtractionProvenance(parser="azure_document_intelligence"),
    )
    table_cell = CanonicalTableCell(
        row_index=0,
        column_index=0,
        content="Item",
        confidence=0.85,
        region=BoundingRegion(page=1, bounding_box=[10.0, 10.0, 30.0, 20.0]),
    )
    table = CanonicalTable(table_id="table-1", confidence=0.8, cells=[table_cell])
    field = StructuredField(
        name="Total",
        value="42.00",
        confidence=0.95,
        value_type="currency",
        region=BoundingRegion(page=1, bounding_box=[5.0, 5.0, 15.0, 10.0]),
    )
    summary = DocumentSummary(
        summary="Summary text",
        confidence=0.7,
        method="heuristic",
        title="Invoice Summary",
    )
    enrichment = DocumentEnrichment(
        enrichment_type="keywords",
        provider="keyword_service",
        content={"keywords": ["invoice"]},
        confidence=0.6,
    )

    document = CanonicalDocument(
        document_id="doc-1",
        source_uri="s3://bucket/doc.pdf",
        checksum="checksum",
        text_spans=[span],
        tables=[table],
        fields=[field],
        visual_descriptions=[],
        page_segments=[],
        attachments=[
            DocumentAttachment(
                attachment_id="1",
                file_name="note.txt",
                mime_type="text/plain",
            )
        ],
        summaries=[summary],
        enrichments=[enrichment],
        document_type="invoice",
        mime_type="application/pdf",
        metadata={"provider": "azure_document_intelligence", "language": "en"},
    )

    rows = canonical_to_denorm_records(
        document,
        request_id="req-1",
        generated_at=datetime(2024, 1, 1),
    )

    doc_rows = [row for row in rows if isinstance(row, DocRow)]
    assert doc_rows
    assert doc_rows[0].doc_title == "Invoice Summary"
    assert doc_rows[0].status == Status.SUCCEEDED
    assert doc_rows[0].total_blocks >= 2

    page_rows = [row for row in rows if isinstance(row, PageRow)]
    assert page_rows
    assert page_rows[0].page_number == 1
    assert page_rows[0].source_mime == "application/pdf"

    block_rows = [row for row in rows if isinstance(row, BlockRow)]
    assert any(row.block_type == BlockType.PARAGRAPH for row in block_rows)
    assert any(row.block_type == BlockType.TABLE for row in block_rows)
    assert any(row.block_type == BlockType.KV_PAIR for row in block_rows)

    insight_rows = [row for row in rows if isinstance(row, InsightRow)]
    assert any(row.insight_kind == InsightKind.SUMMARY for row in insight_rows)
    assert any(row.insight_kind == InsightKind.EXTRACTION for row in insight_rows)

    # Attachment without canonical document should still produce a DocRow with partial status
    attachment_rows = [row for row in doc_rows if row.part_id and row.part_id.endswith("attachment-1")]
    assert attachment_rows
    assert attachment_rows[0].status == Status.PARTIAL
