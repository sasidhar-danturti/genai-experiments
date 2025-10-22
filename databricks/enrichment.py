"""Enrichment provider interfaces and dispatch utilities."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Protocol, Sequence, Tuple

from parsers.canonical_schema import CanonicalDocument, DocumentEnrichment

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EnrichmentRequest:
    """Normalized request payload provided to enrichment providers."""

    document_id: str
    document: CanonicalDocument
    payload: Dict[str, Any]
    timeout_seconds: Optional[float] = None

    @classmethod
    def from_document(
        cls, document: CanonicalDocument, *, timeout_seconds: Optional[float] = None
    ) -> "EnrichmentRequest":
        return cls(
            document_id=document.document_id,
            document=document,
            payload=document.to_dict(),
            timeout_seconds=timeout_seconds,
        )


@dataclass
class EnrichmentResponse:
    """Response returned by enrichment providers for a batch request."""

    document_id: str
    enrichments: Sequence[Mapping[str, Any]]
    raw_response: Optional[Any] = None
    duration_ms: Optional[int] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class EnrichmentProvider(Protocol):
    """Interface describing a callable enrichment provider."""

    name: str

    @property
    def max_batch_size(self) -> int:
        """Maximum number of requests that can be sent in a single invocation."""

    @property
    def timeout_seconds(self) -> Optional[float]:
        """Maximum time the provider call should take before being considered failed."""

    def enrich(self, requests: Sequence[EnrichmentRequest]) -> Sequence[EnrichmentResponse]:
        """Execute an enrichment call and return responses for each request."""


class EnrichmentDispatcher:
    """Coordinates calls to enrichment providers and normalises responses."""

    def __init__(self, providers: Sequence[EnrichmentProvider]):
        self._providers = {provider.name: provider for provider in providers}

    def dispatch(
        self,
        documents: Sequence[CanonicalDocument],
        provider_names: Iterable[str],
    ) -> Dict[str, List[DocumentEnrichment]]:
        """Dispatch enrichment calls for the provided documents."""

        if not documents:
            return {}

        results: Dict[str, List[DocumentEnrichment]] = {
            document.document_id: [] for document in documents
        }

        for provider_name in provider_names:
            provider = self._providers.get(provider_name)
            if provider is None:
                logger.warning("Requested enrichment provider %s is not configured", provider_name)
                continue

            requests = [
                EnrichmentRequest.from_document(
                    document, timeout_seconds=getattr(provider, "timeout_seconds", None)
                )
                for document in documents
            ]
            for batch in _chunked(requests, max(1, _safe_max_batch(provider))):
                responses, default_duration = self._invoke_provider(provider, batch)
                for response in responses:
                    document_id = response.document_id
                    if document_id not in results:
                        logger.warning(
                            "Provider %s returned enrichment for unknown document %s",
                            provider.name,
                            document_id,
                        )
                        continue
                    normalised = self._coerce_enrichments(
                        provider_name,
                        response,
                        default_duration=default_duration,
                    )
                    results[document_id].extend(normalised)

        return results

    def _invoke_provider(
        self,
        provider: EnrichmentProvider,
        batch: Sequence[EnrichmentRequest],
    ) -> Tuple[Sequence[EnrichmentResponse], Optional[int]]:
        if not batch:
            return [], None

        timeout = getattr(provider, "timeout_seconds", None)
        start = time.perf_counter()

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(provider.enrich, batch)
            try:
                responses = future.result(timeout=timeout)
            except FuturesTimeoutError:
                logger.warning(
                    "Enrichment provider %s timed out after %.2fs",
                    provider.name,
                    timeout or 0.0,
                )
                return [], None
            except Exception:  # pragma: no cover - defensive
                logger.exception("Enrichment provider %s failed", provider.name)
                return [], None

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        normalised_responses = list(responses or [])
        return normalised_responses, elapsed_ms

    def _coerce_enrichments(
        self,
        provider_name: str,
        response: EnrichmentResponse,
        *,
        default_duration: Optional[int],
    ) -> List[DocumentEnrichment]:
        enrichments: List[DocumentEnrichment] = []
        entries = response.enrichments or []
        for entry in entries:
            enrichment = self._normalise_entry(
                provider_name,
                entry,
                response_metadata=response.metadata,
                raw_response=response.raw_response,
                explicit_duration=response.duration_ms,
                default_duration=default_duration,
            )
            if enrichment is not None:
                enrichments.append(enrichment)
        if not entries:
            logger.debug(
                "Provider %s returned no enrichment entries for document %s",
                provider_name,
                response.document_id,
            )
        return enrichments

    def _normalise_entry(
        self,
        provider_name: str,
        entry: Mapping[str, Any],
        *,
        response_metadata: Mapping[str, Any],
        raw_response: Optional[Any],
        explicit_duration: Optional[int],
        default_duration: Optional[int],
    ) -> Optional[DocumentEnrichment]:
        if not isinstance(entry, Mapping):
            logger.warning(
                "Enrichment entry from provider %s is not a mapping: %r",
                provider_name,
                entry,
            )
            return None

        enrichment_type = entry.get("enrichment_type") or entry.get("type")
        if not isinstance(enrichment_type, str) or not enrichment_type:
            logger.warning(
                "Enrichment entry from provider %s missing enrichment_type", provider_name
            )
            return None

        content = entry.get("content") or entry.get("payload") or entry.get("data")
        if content is None:
            content = {}
        if not isinstance(content, Mapping):
            logger.warning(
                "Enrichment entry from provider %s has non-mapping content", provider_name
            )
            return None

        model = entry.get("model")
        if model is not None and not isinstance(model, str):
            logger.warning("Enrichment entry from provider %s has invalid model", provider_name)
            model = None

        confidence = entry.get("confidence")
        confidence_value: Optional[float]
        if confidence is None:
            confidence_value = None
        else:
            try:
                confidence_value = float(confidence)
            except (TypeError, ValueError):
                logger.warning(
                    "Enrichment entry from provider %s has invalid confidence", provider_name
                )
                confidence_value = None

        metadata_payload = entry.get("metadata") or {}
        if isinstance(metadata_payload, Mapping):
            metadata: MutableMapping[str, Any] = dict(metadata_payload)
        else:
            logger.warning(
                "Enrichment entry from provider %s has non-mapping metadata", provider_name
            )
            metadata = {}

        if response_metadata:
            metadata.setdefault("response_metadata", dict(response_metadata))
        if raw_response is not None:
            metadata.setdefault("raw_response", raw_response)

        duration_ms = explicit_duration if explicit_duration is not None else default_duration

        if duration_ms is not None:
            metadata.setdefault("duration_ms", duration_ms)

        return DocumentEnrichment(
            enrichment_type=enrichment_type,
            provider=provider_name,
            content=dict(content),
            confidence=confidence_value,
            model=model,
            duration_ms=duration_ms,
            metadata=dict(metadata),
        )


def _chunked(items: Sequence[EnrichmentRequest], size: int) -> Iterable[Sequence[EnrichmentRequest]]:
    if size <= 0:
        size = 1
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _safe_max_batch(provider: EnrichmentProvider) -> int:
    try:
        return max(1, int(provider.max_batch_size))
    except Exception:  # pragma: no cover - defensive
        logger.warning("Provider %s reported invalid max_batch_size", provider.name)
        return 1

