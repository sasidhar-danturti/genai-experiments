from idp_service.llm_document_intelligence_proxy import LLMAzureDocumentIntelligenceClient
from parsers.adapters.azure_document_intelligence import AzureDocumentIntelligenceAdapter


def test_llm_proxy_generates_canonical_csv_payload():
    client = LLMAzureDocumentIntelligenceClient()
    payload = b"Name,Value\nAlice,10\nBob,20\n"

    poller = client.begin_analyze_document("prebuilt-table", payload, content_type="text/csv")
    adapter = AzureDocumentIntelligenceAdapter()
    canonical = adapter.transform(
        poller.result(),
        document_id="csv-doc-1",
        source_uri="s3://bucket/sample.csv",
        checksum="checksum",
        metadata={"mime_type": "text/csv"},
    )

    assert canonical.text_spans, "Proxy should provide text spans"
    assert canonical.tables, "Proxy should provide a tabular representation"
    table = canonical.tables[0]
    assert table.cells[0].content == "Name"
    assert {cell.content for cell in table.cells} >= {"Alice", "Bob", "10", "20"}
