# Intelligent Document Processing (IDP) Service Overview

This document walks through the end-to-end architecture that powers the IDP service showcased in the demo notebook. It explains how documents travel from ingestion to enrichment, the reasoning behind each design decision, and where to extend the system.

## 1. High-level flow

1. **Batch ingestion** pulls work items from Amazon SQS, batches them, and records metadata in Delta Lake for observability.【F:idp_service/sqs_batch_ingestion.py†L1-L181】
2. **Routing** analyses document content and metadata, determines the appropriate parser strategy, and honours request or configuration overrides.【F:idp_service/sqs_batch_ingestion.py†L183-L200】【F:idp_service/routing/router.py†L714-L915】
3. **Parsing** submits the document to Azure Document Intelligence (or the notebook proxy), converts the response to the canonical schema, and persists the result idempotently.【F:idp_service/document_intelligence_workflow.py†L43-L170】
4. **Summarisation and titling** enrich the canonical record with a short summary and title using Azure OpenAI when available, falling back to deterministic heuristics.【F:idp_service/document_intelligence_workflow.py†L145-L166】【F:idp_service/summarization.py†L23-L195】
5. **Optional external enrichment** calls out to additional services on demand and merges their results into the canonical document.【F:idp_service/document_intelligence_workflow.py†L153-L166】【F:idp_service/enrichment.py†L16-L190】
6. **Persistence & telemetry** write final outputs to Delta tables and publish structured events/CloudWatch metrics for operations dashboards.【F:idp_service/document_intelligence_workflow.py†L168-L169】【F:idp_service/document_intelligence_storage.py†L25-L62】【F:idp_service/observability.py†L1-L179】

## 2. Ingestion and batching

The `idp_service.sqs_batch_ingestion` module is orchestrated as a Databricks job. It creates resilient clients for SQS/S3, supports configurable batch sizes, and repeatedly polls for work so the pipeline remains asynchronous.【F:idp_service/sqs_batch_ingestion.py†L62-L136】 Incoming messages are persisted to a Delta metadata table with timestamps, enabling replay and audit trails for every batch.【F:idp_service/sqs_batch_ingestion.py†L143-L168】 Failure records are captured separately to protect the main pipeline and simplify triage.【F:idp_service/sqs_batch_ingestion.py†L169-L180】

**Design decision:** Keep ingestion lightweight and configuration-driven. The `IngestionConfig` dataclass exposes queue URLs, routing thresholds, override tables, and secrets configuration so operations teams can tune behaviour without code changes.【F:idp_service/sqs_batch_ingestion.py†L62-L93】

## 3. Routing strategies and overrides

Routing happens through the `DocumentRouter`, which builds a `DocumentDescriptor`, fetches inline content when available, and produces a `DocumentAnalysis` summarising the chosen strategy and the layout metrics that led to the decision.【F:idp_service/routing/router.py†L210-L277】【F:idp_service/routing/router.py†L714-L756】 Those analytics can be written back to Delta via `DocumentAnalysis.to_metadata_record`, giving observability into every routing decision.【F:idp_service/routing/router.py†L153-L207】

Multiple analyser options coexist:

* **Heuristic analyser** uses metadata provided by upstream systems when layout pages are already calculated.【F:idp_service/routing/router.py†L362-L393】
* **PyMuPDF analyser** inspects PDFs/emails locally, counting tables, images, checkboxes, and radio buttons to spot forms and scans.【F:idp_service/routing/router.py†L461-L521】
* **Model-backed analyser** delegates to external CV models with a fallback to heuristics if the API fails.【F:idp_service/routing/router.py†L395-L412】

The router categorises documents by comparing layout ratios and text density against configurable thresholds to distinguish short form, long form, tables, forms, and scanned documents.【F:idp_service/routing/router.py†L824-L846】

**Override precedence** ensures deterministic behaviour:

1. Request-level overrides immediately force a strategy, honouring caller intent.【F:idp_service/routing/router.py†L848-L860】
2. Pattern overrides apply to filename or metadata markers for compliance-driven routing.【F:idp_service/routing/router.py†L862-L873】
3. Static mode can pin the router to a single parser for whole environments.【F:idp_service/routing/router.py†L784-L799】
4. Default category strategies (with page-count redirects) provide automatic selection otherwise.【F:idp_service/routing/router.py†L877-L914】

## 4. Parser orchestration and idempotency

`DocumentIntelligenceWorkflow` manages retries against Azure Document Intelligence, checksum-based idempotency, canonical transformation, summarisation, enrichment, and final persistence.【F:idp_service/document_intelligence_workflow.py†L55-L169】 By hashing the document payload before submission, the workflow skips redundant work when the same payload appears again unless the caller forces reprocessing.【F:idp_service/document_intelligence_workflow.py†L121-L129】 Wrapping the Azure SDK in `AzureDocumentIntelligenceService` centralises retry policy so intermittent API failures do not bubble up to the caller.【F:idp_service/document_intelligence_workflow.py†L55-L81】

The canonical schema comes from `parsers.adapters` and `parsers.canonical_schema`, allowing downstream systems to consume a uniform JSON structure independent of which parser generated it.【F:idp_service/document_intelligence_workflow.py†L11-L14】 Persisting the canonical record through `DocumentResultStore` implementations keeps the workflow plug-and-play between in-memory unit tests and Delta Lake in production.【F:idp_service/document_intelligence_workflow.py†L33-L40】【F:idp_service/document_intelligence_storage.py†L25-L62】

## 5. Azure Document Intelligence proxy

When Azure Document Intelligence is unavailable (e.g., local development), the notebook swaps in `LLMAzureDocumentIntelligenceClient`, which mimics the Azure SDK surface area and produces realistic `analyzeResult` payloads across multiple MIME types.【F:idp_service/llm_document_intelligence_proxy.py†L31-L185】 The proxy reuses PyMuPDF for PDFs, OpenPyXL for spreadsheets, and custom logic for emails/CSVs to emit paragraph, table, and page structures with confidence scores so the downstream adapters behave exactly as they would against Azure.【F:idp_service/llm_document_intelligence_proxy.py†L93-L185】

## 6. Summaries and titles

`DefaultDocumentSummarizer` first attempts to call Azure OpenAI (through the flexible chat completion interface) and falls back to deterministic leading-sentence summarisation when the API cannot be reached.【F:idp_service/summarization.py†L32-L195】 Both branches populate the canonical `DocumentSummary` structure with the generated summary, inferred title, justification, and confidence so consumers can judge quality.【F:idp_service/summarization.py†L85-L195】

## 7. Optional external enrichment

Callers can pass a list of enrichment provider names to the workflow. The `EnrichmentDispatcher` batches requests, enforces provider-specific timeouts, and normalises responses into `DocumentEnrichment` entries with consistent metadata (provider, duration, payload).【F:idp_service/document_intelligence_workflow.py†L153-L166】【F:idp_service/enrichment.py†L16-L190】 Providers advertise maximum batch sizes and timeouts via the protocol, allowing high-throughput extensions without touching the workflow core.【F:idp_service/enrichment.py†L48-L126】

## 8. Persistence and observability

The default `DeltaDocumentResultStore` appends canonical records to a Delta table, while tests rely on `InMemoryDocumentResultStore` for fast execution without Spark.【F:idp_service/document_intelligence_storage.py†L25-L62】 Structured logging via `StructuredEventLogger` records job lifecycle events in Delta/MLflow, and `CloudWatchMetricsEmitter` publishes queue depth and success metrics for live monitoring dashboards.【F:idp_service/observability.py†L1-L179】 Together with the routing metadata, these artefacts make it easy to trace a document across ingestion, routing, parsing, and enrichment.

## 9. Notebook experience

The `notebooks/idp_end_to_end_demo.ipynb` notebook orchestrates the same components used in production. It populates sample documents at runtime (using embedded base64 fixtures), walks through routing outcomes, executes the proxy-backed workflow, and demonstrates enrichment hooks. Because the notebook reuses the production modules, it doubles as executable documentation for onboarding and regression testing. For more exhaustive coverage, `notebooks/idp_validation_suite.ipynb` runs a scenario matrix that exercises overrides, enrichment batching, and idempotent replays to serve as a regression harness. When you need to rehearse the production experience (including batch orchestration and TODO-marked integration points), open `notebooks/idp_production_simulation.ipynb`, which mirrors the Databricks job flow while showing exactly where to inject Azure credentials, enrichment endpoints, and Delta persistence.

## 10. Extensibility tips

* **Adding new parsers:** implement a `ParserAdapter` for the target service and register a routing strategy that points to it. The canonical schema ensures downstream components require no changes.【F:idp_service/document_intelligence_workflow.py†L50-L169】【F:idp_service/routing/router.py†L877-L914】
* **Introducing new enrichment providers:** implement the `EnrichmentProvider` protocol and pass the instance into `WorkflowConfig.enrichment_providers` so the dispatcher can invoke it.【F:idp_service/document_intelligence_workflow.py†L43-L109】【F:idp_service/enrichment.py†L48-L190】
* **Tuning routing:** adjust thresholds or overrides in the configuration tables/secrets; no code changes are needed because `RouterConfig` converts dictionaries into structured strategy configs at runtime.【F:idp_service/routing/router.py†L307-L359】

Together these components deliver an asynchronous, observable, and easily extensible IDP platform that can grow with new document types, parsers, and downstream insights.
