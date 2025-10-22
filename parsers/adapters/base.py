"""Common adapter interfaces."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from ..canonical_schema import CanonicalDocument


class AdapterError(RuntimeError):
    """Raised when an adapter fails to construct a canonical payload."""


class ParserAdapter(ABC):
    """Base interface for adapters."""

    @abstractmethod
    def transform(
        self,
        payload: Any,
        *,
        document_id: str,
        source_uri: str,
        checksum: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CanonicalDocument:
        """Transform a parser payload into the canonical document schema."""

    @staticmethod
    def _normalise_confidence(confidence: Any) -> float:
        """Normalise optional confidence values to a float."""

        if confidence is None:
            return 1.0
        try:
            return float(confidence)
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
            raise AdapterError("Confidence values must be numeric") from exc
