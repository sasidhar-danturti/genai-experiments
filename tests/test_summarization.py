import json

from parsers.canonical_schema import CanonicalDocument, CanonicalTextSpan
from databricks.summarization import DefaultDocumentSummarizer


class _FakeAzureClient:
    def __init__(self, response):
        self._response = response
        self.chat = self._Chat(self)

    class _Chat:
        def __init__(self, outer):
            self.completions = _FakeAzureClient._Completions(outer)

    class _Completions:
        def __init__(self, outer):
            self._outer = outer
            self.calls = []

        def create(self, **kwargs):
            self.calls.append(kwargs)
            return self._outer._response


def _base_document(text: str) -> CanonicalDocument:
    span = CanonicalTextSpan(content=text, confidence=0.9)
    return CanonicalDocument(
        document_id="doc-1",
        source_uri="uri",
        checksum="checksum",
        text_spans=[span],
        tables=[],
        fields=[],
    )


def test_default_summarizer_fallback():
    document = _base_document("This is the first sentence. This is the second sentence providing more detail.")
    summarizer = DefaultDocumentSummarizer()

    summaries = summarizer.summarise(document)

    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.method == "heuristic_leading_sentences"
    assert "first sentence" in summary.summary
    assert summary.confidence == 0.3


def test_default_summarizer_uses_azure_when_available():
    payload = {
        "choices": [
            {
                "message": {
                    "content": json.dumps(
                        {
                            "summary": "Concise overview of the document.",
                            "title": "Document Title",
                            "confidence": 0.85,
                            "justification": "Model score",
                            "metadata": {"source": "azure"},
                        }
                    )
                }
            }
        ]
    }
    client = _FakeAzureClient(payload)
    summarizer = DefaultDocumentSummarizer(azure_client=client, deployment_name="gpt-summarizer")
    document = _base_document("An example text span for Azure summarisation.")

    summaries = summarizer.summarise(document)

    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.method == "azure_openai"
    assert summary.title == "Document Title"
    assert summary.model == "gpt-summarizer"
    assert summary.confidence == 0.85
    assert summary.metadata == {"source": "azure"}
