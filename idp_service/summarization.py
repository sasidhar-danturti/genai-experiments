"""Summarisation utilities for canonical documents."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence

from parsers.canonical_schema import CanonicalDocument, CanonicalTextSpan, DocumentSummary

logger = logging.getLogger(__name__)


class DocumentSummarizer(Protocol):
    """Protocol for components capable of summarising canonical documents."""

    def summarise(self, document: CanonicalDocument) -> List[DocumentSummary]:
        ...


@dataclass
class DefaultDocumentSummarizer:
    """Summarise canonical documents using Azure OpenAI with deterministic fallback."""

    azure_client: Optional[Any] = None
    deployment_name: Optional[str] = None
    temperature: float = 0.0
    max_input_characters: int = 6000

    def summarise(self, document: CanonicalDocument) -> List[DocumentSummary]:
        text = self._normalised_text(document.text_spans)
        if not text:
            return []

        azure_summary = self._summarise_with_azure(text, document)
        if azure_summary:
            return [azure_summary]

        fallback = self._heuristic_summary(document, text)
        return [fallback] if fallback else []

    # ------------------------------------------------------------------
    # Azure OpenAI summarisation
    # ------------------------------------------------------------------

    def _summarise_with_azure(self, text: str, document: CanonicalDocument) -> Optional[DocumentSummary]:
        if not self.azure_client or not self.deployment_name:
            return None

        try:
            response = self._invoke_chat_completion(
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are an assistant that produces short factual summaries and titles "
                            "for enterprise documents. Respond with JSON containing 'summary', "
                            "'title', 'confidence', and 'justification'."
                        ),
                    },
                    {
                        "role": "user",
                        "content": text,
                    },
                ]
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Azure OpenAI summarisation failed", exc_info=exc)
            return None

        if response is None:
            return None

        try:
            payload = self._extract_json_payload(response)
        except ValueError as exc:
            logger.warning("Unable to parse Azure OpenAI response", exc_info=exc)
            return None

        if not payload:
            return None

        summary_text = _clean_str(payload.get("summary"))
        if not summary_text:
            return None

        title = _clean_str(payload.get("title"))
        justification = _clean_str(payload.get("justification") or payload.get("reasoning"))
        confidence = payload.get("confidence")
        try:
            confidence_value = float(confidence) if confidence is not None else 0.7
        except (TypeError, ValueError):
            confidence_value = 0.7

        model_name = _clean_str(payload.get("model")) or self.deployment_name
        metadata: Dict[str, Any] = {}
        if isinstance(payload.get("metadata"), dict):
            metadata = dict(payload["metadata"])  # type: ignore[arg-type]

        return DocumentSummary(
            summary=summary_text,
            title=title,
            confidence=confidence_value,
            method="azure_openai",
            model=model_name,
            justification=justification,
            metadata=metadata,
        )

    def _invoke_chat_completion(self, messages: Sequence[Dict[str, str]]) -> Optional[Any]:
        client = self.azure_client
        if client is None:
            return None

        callables: Iterable[Any]
        if hasattr(client, "chat") and hasattr(client.chat, "completions"):
            callables = [client.chat.completions.create]
        else:
            callables = [getattr(client, "create", None)]

        kwargs = {
            "messages": messages,
            "temperature": self.temperature,
            "response_format": {"type": "json_object"},
        }

        for create_fn in callables:
            if create_fn is None:
                continue
            for param_name in ("model", "deployment_id", "deployment_name"):
                try:
                    response = create_fn(**{param_name: self.deployment_name, **kwargs})
                    return response
                except TypeError:
                    continue
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("Azure OpenAI call failed", exc_info=exc)
                    return None
        return None

    @staticmethod
    def _extract_json_payload(response: Any) -> Dict[str, Any]:
        choices = getattr(response, "choices", None)
        if choices is None and isinstance(response, dict):
            choices = response.get("choices")
        if not choices:
            raise ValueError("No choices returned from Azure OpenAI")

        choice = choices[0]
        message = getattr(choice, "message", None)
        if message is None and isinstance(choice, dict):
            message = choice.get("message") or {}
        content = getattr(message, "content", None)
        if content is None:
            if isinstance(message, dict):
                content = message.get("content")
        if content is None and isinstance(choice, dict):
            content = choice.get("content")
        if not content:
            raise ValueError("Azure OpenAI choice missing content")

        if isinstance(content, list):
            content = "".join(fragment.get("text", "") for fragment in content if isinstance(fragment, dict))

        if not isinstance(content, str):
            raise ValueError("Azure OpenAI content must be a string")

        return json.loads(content)

    # ------------------------------------------------------------------
    # Fallback heuristics
    # ------------------------------------------------------------------

    def _heuristic_summary(self, document: CanonicalDocument, text: str) -> Optional[DocumentSummary]:
        sentences = self._split_sentences(text)
        if not sentences:
            return None
        summary_sentences = sentences[:2]
        summary_text = " ".join(summary_sentences)
        if len(summary_text) > 512:
            summary_text = summary_text[:512].rsplit(" ", 1)[0]

        title = self._infer_title(document.text_spans)
        justification = "Generated via deterministic leading-sentence heuristic fallback."

        return DocumentSummary(
            summary=summary_text,
            title=title,
            confidence=0.3,
            method="heuristic_leading_sentences",
            model=None,
            justification=justification,
        )

    def _normalised_text(self, spans: Iterable[CanonicalTextSpan]) -> str:
        contents: List[str] = []
        seen = set()
        for span in spans:
            content = _clean_str(getattr(span, "content", ""))
            if not content:
                continue
            if content in seen:
                continue
            seen.add(content)
            contents.append(content)

        if not contents:
            return ""

        normalised = "\n".join(contents)
        if len(normalised) > self.max_input_characters:
            normalised = normalised[: self.max_input_characters]
        return normalised

    @staticmethod
    def _split_sentences(text: str) -> List[str]:
        if not text:
            return []
        sentences = [segment.strip() for segment in re.split(r"(?<=[.!?])\s+", text) if segment.strip()]
        if not sentences:
            sentences = [text.strip()]
        return sentences

    @staticmethod
    def _infer_title(spans: Iterable[CanonicalTextSpan]) -> Optional[str]:
        for span in spans:
            content = _clean_str(getattr(span, "content", ""))
            if not content:
                continue
            if len(content) <= 120 and content.count(" ") <= 15:
                return content
        return None


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value).strip() or None

