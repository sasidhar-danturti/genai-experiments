import sys
from pathlib import Path

# Allow tests to import the project packages without installation.
sys.path.append(str(Path(__file__).resolve().parents[1]))

from idp_service.enrichment import EnrichmentDispatcher, EnrichmentResponse  # noqa: E402
from parsers.canonical_schema import (  # noqa: E402
    CanonicalDocument,
)


def _document(document_id: str) -> CanonicalDocument:
    return CanonicalDocument(
        document_id=document_id,
        source_uri=f"s3://bucket/{document_id}.pdf",
        checksum=f"checksum-{document_id}",
        text_spans=[],
        tables=[],
        fields=[],
    )


class _BatchingProvider:
    def __init__(self, *, name: str = "keywords", max_batch_size: int = 2):
        self.name = name
        self.max_batch_size = max_batch_size
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
                        }
                    ],
                )
            )
        return responses


def test_dispatcher_batches_requests():
    documents = [_document(f"doc-{index}") for index in range(3)]
    provider = _BatchingProvider(max_batch_size=2)
    dispatcher = EnrichmentDispatcher([provider])

    result = dispatcher.dispatch(documents, [provider.name])

    assert len(provider.calls) == 2
    assert provider.calls[0] == ["doc-0", "doc-1"]
    assert provider.calls[1] == ["doc-2"]
    for document in documents:
        enrichments = result[document.document_id]
        assert len(enrichments) == 1
        enrichment = enrichments[0]
        assert enrichment.enrichment_type == "keywords"
        assert enrichment.provider == provider.name
        assert enrichment.content["keywords"] == [document.document_id]


def test_dispatcher_ignores_invalid_payloads(caplog):
    class _InvalidProvider(_BatchingProvider):
        def enrich(self, requests):
            super().enrich(requests)
            return [
                EnrichmentResponse(
                    document_id=requests[0].document_id,
                    enrichments=[{"type": None, "content": "invalid"}],
                )
            ]

    document = _document("doc-invalid")
    provider = _InvalidProvider()
    dispatcher = EnrichmentDispatcher([provider])

    with caplog.at_level("WARNING"):
        result = dispatcher.dispatch([document], [provider.name])

    assert provider.calls
    assert result[document.document_id] == []
    assert any("missing enrichment_type" in message for message in caplog.messages)
