# IDP Router Library

The `idp_router` package provides the document routing logic that powers our ingestion stack.  It can now be installed as an independent library and embedded in any orchestration service that needs to choose the right parser or downstream workflow for a document.

This guide explains every public type in the module, demonstrates how the router makes its decisions, and provides runnable samples for all of the execution paths that the `DocumentRouter` can take.

## Package layout

```
idp_router/
├── __init__.py                  # Compact export surface for reuse
├── layout/
│   └── huggingface.py           # Optional Hugging Face LayoutLM integration
└── router.py                    # Core routing and layout analysis logic
```

Importing from `idp_router` re-exports every public class from `router.py` so that consumers can write concise code such as `from idp_router import DocumentRouter, RouterConfig`.

## Key concepts and classes

The following sections group the public API into related areas.  Each entry explains the purpose of the class or enum and how it participates in the routing pipeline.

### Enumerations

| Class | Purpose |
| --- | --- |
| `RoutingMode` | Selects between `HYBRID` (heuristics + overrides) and `STATIC` (always use the configured static strategy). |
| `DocumentCategory` | Canonical categories (`SHORT_FORM`, `LONG_FORM`, `SCANNED`, `TABLE_HEAVY`, `FORM_HEAVY`, `UNKNOWN`) inferred from layout metrics to drive downstream parser selection. |
| `LayoutModelType` | Identifiers for remote CV services (currently LayoutLM v3, DocFormer, Table DETR, and a form classifier) used by `RequestsLayoutModelClient`. |

### Strategy and override types

| Class | Purpose |
| --- | --- |
| `StrategyConfig` | Declarative configuration describing a candidate parser (name, optional model identifier, optional page cap). |
| `PatternOverride` | Couples a compiled regular expression with a `StrategyConfig` so that file-name or metadata patterns can redirect routing. |
| `OverrideSet` | Aggregates a sequence of `PatternOverride` objects that are evaluated in order before any automatic routing takes place. |
| `ParserStrategy` | Concrete routing decision returned by the router; captures the `name`, `reason`, and optional model/max page values that should be executed downstream. |

### Document analysis data structures

| Class | Purpose |
| --- | --- |
| `PageMetrics` | Atomic layout summary for a page (density metrics, character counts, table/image/check-box counts). |
| `DocumentProfile` | Aggregates per-page metrics into document-level ratios and totals that power categorisation. |
| `DocumentAnalysis` | Final output of `DocumentRouter.route`; combines profile statistics, selected strategy, and traceability metadata ready to be persisted. |
| `DocumentDescriptor` | Lightweight container for raw routing inputs (object key, bucket, request payload, MIME type, request-level overrides). |

### Layout analysers and model clients

| Class | Purpose |
| --- | --- |
| `LayoutAnalyser` | Protocol describing the interface used by the router to obtain `DocumentProfile` objects. |
| `HeuristicLayoutAnalyser` | Interprets metadata embedded in the request payload when no external model is available. |
| `ModelBackedLayoutAnalyser` | Wraps a `LayoutModelClient` (local or remote) and falls back to heuristics if inference fails. |
| `PyMuPDFLayoutAnalyser` | Uses `PyMuPDF` to inspect PDFs and Outlook email packages when the raw bytes are available. |
| `LayoutModelClient` | Protocol for deep-learning or remote inference clients that emit `PageMetrics`. |
| `RequestsLayoutModelClient` | Minimal HTTP client that calls a remote layout analysis service, forwarding metadata and optional inline document bytes. |
| `HuggingFaceLayoutModelClient` | Optional dependency that transforms documents into images, runs OCR, and feeds LayoutLM models to produce metrics.  Requires `pillow`, `pytesseract`, `torch`, `transformers`, and (for PDFs) `pymupdf` at runtime. |

### Content resolution utilities

| Class | Purpose |
| --- | --- |
| `ContentResolver` | Protocol that supplies raw document bytes to analysers that can benefit from them. |
| `InlineDocumentContentResolver` | Default resolver that extracts inline base64 payloads or embedded metadata blobs from the routing request.

### Router configuration and orchestration

| Class | Purpose |
| --- | --- |
| `RouterConfig` | Captures routing thresholds, default strategies per category, optional static/fallback strategies, and request override flags. |
| `DocumentRouter` | Central coordinator that builds descriptors, resolves content, analyses layout, categorises the document, applies overrides, and returns a `DocumentAnalysis` ready for downstream processing.

## Execution paths and sample code

The samples below demonstrate each way the router can arrive at a decision.  They can be pasted into a Python shell or the accompanying notebook.

All examples share a helper request payload and a base configuration:

```python
from idp_router import (
    DocumentRouter,
    HeuristicLayoutAnalyser,
    ModelBackedLayoutAnalyser,
    PyMuPDFLayoutAnalyser,
    RequestsLayoutModelClient,
    RouterConfig,
    RoutingMode,
    OverrideSet,
    PatternOverride,
    StrategyConfig,
    DocumentCategory,
)

sample_body = {
    "documentMetadata": {
        "layout": {
            "pages": [
                {
                    "textDensity": 0.8,
                    "imageDensity": 0.05,
                    "tableDensity": 0.2,
                    "tableCount": 1,
                }
            ]
        }
    }
}

base_config = RouterConfig(
    default_strategy_map={
        DocumentCategory.SHORT_FORM.value: {"name": "azure_form_recognizer"},
        DocumentCategory.LONG_FORM.value: {"name": "textract_async"},
        DocumentCategory.TABLE_HEAVY.value: {"name": "table_extractor"},
        DocumentCategory.FORM_HEAVY.value: {"name": "form_specialist"},
    },
    fallback_strategy={"name": "generic_ocr"},
)
```

### 1. Hybrid routing with heuristics (default path)

```python
router = DocumentRouter(base_config, HeuristicLayoutAnalyser())
analysis = router.route(sample_body, object_key="invoices/acme-001.pdf", overrides=OverrideSet())
print(analysis.strategy.name)  # -> "azure_form_recognizer"
print(analysis.overrides_applied)  # -> ["category_default"]
```

The router uses the heuristics embedded in the payload to categorise the document and selects the category default.

### 2. Static routing mode

```python
static_config = RouterConfig(
    mode=RoutingMode.STATIC,
    static_strategy={"name": "force_textract", "model": "textract-v1"},
    default_strategy_map={},
)

router = DocumentRouter(static_config, HeuristicLayoutAnalyser())
analysis = router.route(sample_body, "contracts/nda.pdf", OverrideSet())
print(analysis.strategy.name)  # -> "force_textract"
print(analysis.overrides_applied)  # -> ["static_config"]
```

In `STATIC` mode the router ignores categories and always returns the configured strategy.

### 3. Request-level override flag

```python
override_body = {
    **sample_body,
    "parser_override": {"name": "manual_review", "model": "human-in-the-loop"},
}

router = DocumentRouter(base_config, HeuristicLayoutAnalyser())
analysis = router.route(override_body, "receipts/img-42.png", OverrideSet())
print(analysis.strategy.name)   # -> "manual_review"
print(analysis.overrides_applied)  # -> ["request_override"]
```

If the incoming payload supplies the configured request override flag (default `parser_override`) it takes precedence over all other logic.

### 4. Pattern overrides driven by filename

```python
import re

overrides = OverrideSet(
    pattern_overrides=[
        PatternOverride(
            pattern=re.compile(r"bank_statements/.*\.pdf$"),
            strategy=StrategyConfig(name="bank_statement_parser"),
        )
    ]
)

router = DocumentRouter(base_config, HeuristicLayoutAnalyser())
analysis = router.route(sample_body, "bank_statements/jan.pdf", overrides)
print(analysis.strategy.name)   # -> "bank_statement_parser"
print(analysis.overrides_applied)  # -> ["pattern:bank_statements/.*\\.pdf$"]
```

Pattern overrides are evaluated before automatic categorisation.

### 5. Page-threshold fallback

```python
long_document_body = {
    "documentMetadata": {
        "layout": {
            "pages": [
                {"textDensity": 0.7, "imageDensity": 0.05, "tableDensity": 0.1}
                for _ in range(120)
            ]
        }
    }
}

threshold_config = RouterConfig(
    category_thresholds={"long_form_max_pages": 80},
    fallback_strategy={"name": "asynchronous_pipeline"},
    default_strategy_map={DocumentCategory.LONG_FORM.value: {"name": "sync_pipeline"}},
)

router = DocumentRouter(threshold_config, HeuristicLayoutAnalyser())
analysis = router.route(long_document_body, "legal/contract.pdf", OverrideSet())
print(analysis.strategy.name)  # -> "asynchronous_pipeline"
print(analysis.overrides_applied)  # -> ["category_default", "threshold_redirect"]
```

When the inferred category exceeds its configured max page threshold the router falls back to `fallback_strategy`.

### 6. Remote layout inference with `RequestsLayoutModelClient`

```python
remote_client = RequestsLayoutModelClient(
    endpoint="https://layout-service.internal/route",
    api_key="token-123",
    model_type="layoutlm_v3",
)
model_router = DocumentRouter(base_config, ModelBackedLayoutAnalyser(remote_client))
analysis = model_router.route(sample_body, "invoices/acme-001.pdf", OverrideSet())
```

`ModelBackedLayoutAnalyser` delegates layout extraction to the HTTP service.  If the call fails or returns no metrics it transparently falls back to `HeuristicLayoutAnalyser`.

### 7. PDF/email inspection with `PyMuPDFLayoutAnalyser`

```python
pymupdf_router = DocumentRouter(
    base_config,
    PyMuPDFLayoutAnalyser(),
)
pdf_payload = sample_body | {"documentBytes": "...base64 pdf..."}
analysis = pymupdf_router.route(pdf_payload, "reports/annual.pdf", OverrideSet())
```

`PyMuPDFLayoutAnalyser` attempts to extract real layout metrics from PDFs or Outlook email packages when the raw bytes are supplied (inline or via a resolver).

### 8. Hugging Face LayoutLM integration

```python
from idp_router import HuggingFaceLayoutModelClient

hf_client = HuggingFaceLayoutModelClient(
    model_id="nielsr/layoutlmv3-finetuned-funsd",
    confidence_threshold=0.7,
)
hf_router = DocumentRouter(
    base_config,
    ModelBackedLayoutAnalyser(hf_client, fallback=PyMuPDFLayoutAnalyser()),
)
pdf_bytes = open("/path/to/form.pdf", "rb").read()
analysis = hf_router.route(sample_body | {"documentBytes": pdf_bytes}, "forms/form.pdf", OverrideSet())
```

The Hugging Face client requires raw document bytes and the optional ML dependencies.  Use the provided extras when installing the package (see below).

## Packaging and installation

The repository now includes a `pyproject.toml` that exposes `idp_router` as an installable distribution:

```bash
pip install .
```

Optional extras pull in heavy dependencies only when needed:

- `pip install .[huggingface]` – adds OCR and LayoutLM requirements.
- `pip install .[pymupdf]` – adds PDF/email inspection support.
- `pip install .[all]` – installs every optional dependency.

## Notebook quick start

Open `notebooks/idp_router_quickstart.ipynb` for an executable tour of the API.  The notebook reproduces the samples above, shows how to feed real documents into the router, and captures the resulting `DocumentAnalysis` objects for inspection.

