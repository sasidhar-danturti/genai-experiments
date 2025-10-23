import sys
from pathlib import Path

import pytest

# Allow tests to import the project packages without installation.
sys.path.append(str(Path(__file__).resolve().parents[1]))

from idp_service.document_intelligence_workflow import (  # noqa: E402
    DocumentIntelligenceWorkflow,
    WorkflowConfig,
)
from idp_service.enrichment import EnrichmentResponse  # noqa: E402
from idp_service.document_intelligence_storage import (  # noqa: E402
    InMemoryDocumentResultStore,
)
from parsers.adapters.azure_document_intelligence import (  # noqa: E402
    AzureDocumentIntelligenceAdapter,
)
from parsers.adapters.databricks_llm_image import DatabricksLLMImageAdapter  # noqa: E402
from parsers.adapters.email_parser import EmailParserAdapter  # noqa: E402
from parsers.adapters.multi_parser import MultiParserAdapter  # noqa: E402
from parsers.adapters.pymupdf import PyMuPDFAdapter  # noqa: E402


class _FakePoller:
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result


class _RecordingAzureClient:
    def __init__(self, responses, *, fail_first=False):
        self._responses = list(responses)
        self.calls = []
        self._fail_first = fail_first
        self._attempts = 0

    def begin_analyze_document(self, model_id, document, **kwargs):
        self.calls.append((model_id, document, kwargs))
        if self._fail_first and self._attempts == 0:
            self._attempts += 1
            raise RuntimeError("transient failure")
        if not self._responses:
            raise AssertionError("No responses remaining")
        payload = self._responses.pop(0)
        return _FakePoller(payload)


class _RecordingEnrichmentProvider:
    def __init__(self, *, name: str = "keywords", confidence: float = 0.9):
        self.name = name
        self.confidence = confidence
        self.max_batch_size = 1
        self.timeout_seconds = None
        self.calls = []

    def enrich(self, requests):
        self.calls.append([request.document_id for request in requests])
        responses = []
        for request in requests:
            responses.append(
                EnrichmentResponse(
                    document_id=request.document_id,
                    enrichments=[
                        {
                            "type": "keywords",
                            "content": {"keywords": [request.document_id]},
                            "confidence": self.confidence,
                            "model": "unit-test",
                        }
                    ],
                )
            )
        return responses


@pytest.fixture
def sample_analyze_result():
    return {
        "paragraphs": [
            {
                "content": "Invoice 12345",
                "confidence": 0.9,
                "boundingRegions": [
                    {"pageNumber": 1, "polygon": [0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0]},
                ],
                "id": "para-0",
            }
        ],
        "tables": [
            {
                "id": "table-1",
                "confidence": 0.85,
                "cells": [
                    {
                        "rowIndex": 0,
                        "columnIndex": 0,
                        "content": "item",
                        "confidence": 0.8,
                        "boundingRegions": [{"pageNumber": 1}],
                    }
                ],
                "footnotes": ["example"],
            }
        ],
        "documents": [
            {
                "fields": {
                    "Total": {
                        "value": "100.00",
                        "confidence": 0.99,
                        "type": "currency",
                        "boundingRegions": [{"pageNumber": 1}],
                    }
                }
            }
        ],
    }


@pytest.fixture
def adapter():
    return AzureDocumentIntelligenceAdapter()


def test_adapter_creates_canonical_document(sample_analyze_result, adapter):
    canonical = adapter.transform(
        sample_analyze_result,
        document_id="doc-1",
        source_uri="s3://bucket/doc.pdf",
        checksum="abc123",
        metadata={"source": "unit-test"},
    )

    assert canonical.document_id == "doc-1"
    assert canonical.source_uri == "s3://bucket/doc.pdf"
    assert canonical.metadata["provider"] == "azure_document_intelligence"
    assert canonical.metadata["source"] == "unit-test"
    assert len(canonical.text_spans) == 1
    assert canonical.summaries == []
    assert canonical.text_spans[0].provenance.parser == "azure_document_intelligence"
    assert canonical.text_spans[0].confidence_signals[0].source == "azure_document_intelligence"
    assert canonical.tables[0].cells[0].content == "item"
    assert canonical.fields[0].name == "Total"
    assert canonical.fields[0].confidence == pytest.approx(0.99)
    assert canonical.page_segments[0].parser == "azure_document_intelligence"


def test_workflow_is_idempotent_and_supports_force(sample_analyze_result):
    client = _RecordingAzureClient([sample_analyze_result, sample_analyze_result])
    store = InMemoryDocumentResultStore()
    config = WorkflowConfig(model_id="prebuilt-invoice")

    workflow = DocumentIntelligenceWorkflow(client=client, store=store, config=config)

    result = workflow.process(
        document_id="doc-1",
        document_bytes=b"file-bytes",
        source_uri="abfss://container/doc.pdf",
        metadata={"request_id": "123"},
        pages=[1],
    )

    assert not result.skipped
    assert result.document is not None
    assert result.document.summaries
    assert result.document.summaries[0].method == "heuristic_leading_sentences"
    assert len(client.calls) == 1
    assert client.calls[0][2]["pages"] == [1]

    skipped = workflow.process(
        document_id="doc-1",
        document_bytes=b"file-bytes",
        source_uri="abfss://container/doc.pdf",
        metadata={},
        pages=[1],
    )

    assert skipped.skipped
    assert len(client.calls) == 1

    forced = workflow.process(
        document_id="doc-1",
        document_bytes=b"file-bytes",
        source_uri="abfss://container/doc.pdf",
        metadata={},
        pages=[1],
        force=True,
    )

    assert not forced.skipped
    assert len(client.calls) == 2


def test_workflow_retries_on_transient_failures(sample_analyze_result, monkeypatch):
    client = _RecordingAzureClient([sample_analyze_result], fail_first=True)
    store = InMemoryDocumentResultStore()
    config = WorkflowConfig(model_id="prebuilt-invoice", max_retries=2, retry_backoff_seconds=0)
    workflow = DocumentIntelligenceWorkflow(client=client, store=store, config=config)

    # Avoid actual sleeping during retries
    monkeypatch.setattr("time.sleep", lambda *args, **kwargs: None)

    result = workflow.process(
        document_id="doc-2",
        document_bytes=b"other-bytes",
        source_uri="abfss://container/doc2.pdf",
        metadata={},
    )

    assert not result.skipped
    assert len(client.calls) == 2


def test_databricks_llm_image_adapter_parses_visual_descriptions():
    adapter = DatabricksLLMImageAdapter()
    payload = {
        "overall_description": "A scanned receipt with highlighted total.",
        "text_spans": [
            {
                "content": "Total: $25.00",
                "confidence": 0.92,
                "page": 1,
                "polygon": [0.1, 0.1, 0.4, 0.1, 0.4, 0.2, 0.1, 0.2],
                "id": "text-1",
            }
        ],
        "visual_descriptions": [
            {
                "description": "Customer holding a shopping bag",
                "confidence": 0.88,
                "bounding_box": [0.5, 0.2, 0.9, 0.8],
                "tags": ["person", "shopping"],
            }
        ],
        "fields": [
            {
                "name": "total",
                "value": "25.00",
                "confidence": 0.91,
                "value_type": "currency",
            }
        ],
    }

    canonical = adapter.transform(
        payload,
        document_id="image-1",
        source_uri="dbfs:/images/receipt.png",
        checksum="img-abc",
        metadata={"content_type": "image/png"},
    )

    assert canonical.document_id == "image-1"
    assert canonical.document_type == "image"
    assert canonical.mime_type == "image/png"
    assert canonical.visual_descriptions[0].description == "Customer holding a shopping bag"
    assert canonical.visual_descriptions[0].tags == ["person", "shopping"]
    assert canonical.metadata["provider"] == "databricks_llm_image"
    assert canonical.metadata["overall_description"] == "A scanned receipt with highlighted total."
    assert canonical.fields[0].name == "total"
    assert canonical.text_spans[0].provenance.parser == "databricks_llm_image"
    assert canonical.page_segments[0].parser == "databricks_llm_image"


def test_pymupdf_adapter_handles_pages():
    adapter = PyMuPDFAdapter()
    payload = {
        "pages": [
            {
                "page_number": 1,
                "confidence": 0.94,
                "text_spans": [
                    {"id": "span-1", "content": "Invoice #123", "confidence": 0.91, "bbox": [0, 0, 0.5, 0.5]},
                ],
                "tables": [
                    {
                        "id": "table-1",
                        "confidence": 0.83,
                        "cells": [
                            {
                                "row_index": 0,
                                "column_index": 0,
                                "content": "Widget",
                                "confidence": 0.87,
                                "bbox": [0.1, 0.1, 0.2, 0.2],
                            }
                        ],
                    }
                ],
                "fields": {
                    "Total": {"value": "42.00", "confidence": 0.9, "bbox": [0.2, 0.2, 0.3, 0.3]},
                },
            }
        ],
        "metadata": {"source": "unit-test"},
    }

    canonical = adapter.transform(
        payload,
        document_id="pdf-1",
        source_uri="file:///tmp/invoice.pdf",
        checksum="checksum-1",
        metadata={"mime_type": "application/pdf"},
    )

    assert canonical.metadata["provider"] == "pymupdf"
    assert canonical.text_spans[0].content == "Invoice #123"
    assert canonical.text_spans[0].provenance.parser == "pymupdf"
    assert canonical.tables[0].cells[0].content == "Widget"
    assert canonical.fields[0].name == "Total"
    assert canonical.page_segments[0].parser == "pymupdf"


def test_multi_parser_adapter_merges_outputs():
    ensemble = MultiParserAdapter({
        "pymupdf": PyMuPDFAdapter(),
        "databricks_llm": DatabricksLLMImageAdapter(),
    })

    pymupdf_payload = {
        "pages": [
            {
                "page_number": 1,
                "text_spans": [{"content": "Line from PDF", "confidence": 0.9}],
            }
        ],
    }
    llm_payload = {
        "text_spans": [{"content": "LLM summary", "confidence": 0.88}],
        "visual_descriptions": [],
    }

    payload = {
        "document_metadata": {"mime_type": "application/pdf"},
        "parsers": [
            {"name": "pymupdf", "payload": pymupdf_payload},
            {"name": "databricks_llm", "payload": llm_payload},
        ],
    }

    canonical = ensemble.transform(
        payload,
        document_id="combo-1",
        source_uri="dbfs:/docs/combo.pdf",
        checksum="combo-checksum",
        metadata={"document_type": "statement"},
    )

    assert canonical.metadata["provider"] == "multi_parser"
    assert set(canonical.metadata["parsers_used"]) == {"pymupdf", "databricks_llm_image"}
    assert any(span.provenance.parser == "pymupdf" for span in canonical.text_spans)
    assert any(span.provenance.parser == "databricks_llm_image" for span in canonical.text_spans)
    assert canonical.document_type == "statement"
    assert canonical.mime_type == "application/pdf"


def test_email_parser_supports_attachments():
    adapter = EmailParserAdapter()
    payload = {
        "subject": "Invoice Delivery",
        "body_text": "Hi,\nPlease find the invoice attached.",
        "headers": {"From": "billing@example.com", "To": "ap@example.com"},
        "attachments": [
            {
                "attachment_id": "att-1",
                "file_name": "invoice.pdf",
                "mime_type": "application/pdf",
                "checksum": "abc123",
            }
        ],
    }

    canonical = adapter.transform(
        payload,
        document_id="email-1",
        source_uri="imap://mailbox/message-1",
        checksum="email-checksum",
    )

    assert canonical.document_type == "email"
    assert canonical.metadata["provider"] == "email_parser"
    assert canonical.text_spans[0].provenance.method == "body_text"
    assert canonical.attachments[0].file_name == "invoice.pdf"
    assert canonical.attachments[0].mime_type == "application/pdf"


def test_workflow_triggers_enrichment_when_requested(sample_analyze_result):
    client = _RecordingAzureClient([sample_analyze_result])
    store = InMemoryDocumentResultStore()
    provider = _RecordingEnrichmentProvider()
    config = WorkflowConfig(
        model_id="prebuilt-invoice",
        enrichment_providers=[provider],
    )
    workflow = DocumentIntelligenceWorkflow(client=client, store=store, config=config)

    result = workflow.process(
        document_id="doc-enrich",
        document_bytes=b"file-bytes",
        source_uri="abfss://container/doc.pdf",
        metadata={},
        enrich_with=[provider.name],
    )

    assert provider.calls == [["doc-enrich"]]
    assert result.document is not None
    assert result.document.enrichments
    enrichment = result.document.enrichments[0]
    assert enrichment.enrichment_type == "keywords"
    assert enrichment.provider == provider.name
    assert enrichment.confidence == pytest.approx(provider.confidence)

    persisted = store._records[(result.document.document_id, result.document.checksum)]
    assert persisted.enrichments
