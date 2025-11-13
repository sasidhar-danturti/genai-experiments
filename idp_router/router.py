import base64
import binascii
import io
import json
import logging
import mimetypes
import re
import statistics
import zipfile
from dataclasses import dataclass, field
from email import message_from_bytes
from email.message import Message
from enum import Enum
from html.parser import HTMLParser
from typing import Dict, Iterable, List, Optional, Protocol, Sequence, Tuple
from urllib import request as urllib_request
from urllib.error import URLError

try:  # pragma: no cover - optional dependency for production environments
    import fitz  # type: ignore

    PDF_WIDGET_TYPE_CHECKBOX = getattr(fitz, "PDF_WIDGET_TYPE_CHECKBOX", None)
    PDF_WIDGET_TYPE_RADIOBUTTON = getattr(fitz, "PDF_WIDGET_TYPE_RADIOBUTTON", None)
except ImportError:  # pragma: no cover - optional dependency
    fitz = None
    PDF_WIDGET_TYPE_CHECKBOX = None
    PDF_WIDGET_TYPE_RADIOBUTTON = None

logger = logging.getLogger(__name__)


_INLINE_PAYLOAD_KEYS = (
    "documentBytes",
    "document_bytes",
    "documentContent",
    "document_content",
    "payload",
)

_INLINE_METADATA_KEYS = ("inlineContent", "inline_content")


class RoutingMode(str, Enum):
    """Defines how the :class:`DocumentRouter` should behave."""

    STATIC = "static"
    HYBRID = "hybrid"


class DocumentCategory(str, Enum):
    """High-level categorisation used to route documents to parsers."""

    SHORT_FORM = "short_form"
    LONG_FORM = "long_form"
    SCANNED = "scanned"
    TABLE_HEAVY = "table_heavy"
    FORM_HEAVY = "form_heavy"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class StrategyConfig:
    """Declarative configuration for a parser strategy."""

    name: str
    model: Optional[str] = None
    max_pages: Optional[int] = None

    @classmethod
    def from_mapping(cls, payload: Dict[str, object]) -> "StrategyConfig":
        payload = payload or {}
        return cls(
            name=str(payload.get("name", "general")),
            model=payload.get("model"),
            max_pages=_safe_int(payload.get("max_pages")),
        )


@dataclass
class PatternOverride:
    """Overrides applied based on filename patterns or metadata markers."""

    pattern: re.Pattern
    strategy: StrategyConfig


@dataclass
class OverrideSet:
    """Collection of overrides loaded from configuration/metadata."""

    pattern_overrides: Sequence[PatternOverride] = field(default_factory=list)


@dataclass
class ParserStrategy:
    """Represents the selected parsing strategy for a document."""

    name: str
    reason: str
    model: Optional[str] = None
    max_pages: Optional[int] = None


@dataclass
class PageMetrics:
    """Summary of layout metrics for a single page."""

    index: int
    text_density: float
    image_density: float
    table_density: float
    char_count: Optional[int] = None
    table_count: int = 0
    image_count: int = 0
    checkbox_count: int = 0
    radio_button_count: int = 0

    def to_dict(self) -> Dict[str, object]:
        return {
            "index": self.index,
            "text_density": self.text_density,
            "image_density": self.image_density,
            "table_density": self.table_density,
            "char_count": self.char_count,
            "table_count": self.table_count,
            "image_count": self.image_count,
            "checkbox_count": self.checkbox_count,
            "radio_button_count": self.radio_button_count,
        }


@dataclass
class DocumentProfile:
    """Aggregated profile of a document derived from page metrics."""

    object_key: str
    bucket: Optional[str]
    mime_type: str
    page_count: int
    pages: Sequence[PageMetrics]
    average_text_density: float
    average_image_density: float
    table_page_ratio: float
    scanned_page_ratio: float
    checkbox_page_ratio: float
    radio_button_page_ratio: float
    form_page_ratio: float
    total_tables: int
    total_checkboxes: int
    total_radio_buttons: int


@dataclass
class DocumentAnalysis:
    """Aggregated analysis of a document prior to routing."""

    object_key: str
    mime_type: str
    page_count: int
    category: DocumentCategory
    strategy: ParserStrategy
    overrides_applied: List[str]
    request_override: Optional[str]
    average_text_density: float
    average_image_density: float
    table_page_ratio: float
    scanned_page_ratio: float
    checkbox_page_ratio: float
    radio_button_page_ratio: float
    form_page_ratio: float
    total_tables: int
    total_checkboxes: int
    total_radio_buttons: int
    pages: Sequence[PageMetrics]
    raw_metadata: dict

    def to_metadata_record(self, base_record: dict) -> dict:
        record = base_record.copy()
        record.update(
            {
                "mime_type": self.mime_type,
                "page_count": self.page_count,
                "layout_density": self.average_text_density,
                "image_density": self.average_image_density,
                "table_page_ratio": self.table_page_ratio,
                "scanned_page_ratio": self.scanned_page_ratio,
                "checkbox_page_ratio": self.checkbox_page_ratio,
                "radio_button_page_ratio": self.radio_button_page_ratio,
                "form_page_ratio": self.form_page_ratio,
                "total_tables": self.total_tables,
                "total_checkboxes": self.total_checkboxes,
                "total_radio_buttons": self.total_radio_buttons,
                "document_category": self.category.value,
                "parser_strategy": self.strategy.name,
                "strategy_reason": self.strategy.reason,
                "parser_model": self.strategy.model,
                "strategy_max_pages": self.strategy.max_pages,
                "overrides_applied": ",".join(self.overrides_applied)
                if self.overrides_applied
                else None,
                "request_override": self.request_override,
                "page_metrics": json.dumps([page.to_dict() for page in self.pages])
                if self.pages
                else None,
            }
        )
        return record


@dataclass
class DocumentDescriptor:
    """Raw inputs used for routing decisions."""

    object_key: str
    bucket: Optional[str]
    body: dict
    mime_type: str
    request_override: Optional[str]

    @property
    def source_uri(self) -> Optional[str]:
        if self.bucket and self.object_key:
            return f"s3://{self.bucket}/{self.object_key}"
        return None


def _coerce_bytes(value: object) -> Optional[bytes]:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        try:
            return base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error):  # type: ignore[name-defined]
            try:
                return value.encode("utf-8")
            except Exception:
                return None
    return None


def _extract_inline_bytes(body: object) -> Optional[bytes]:
    if not isinstance(body, dict):
        return None

    for key in _INLINE_PAYLOAD_KEYS:
        if key in body and body[key]:
            payload_bytes = _coerce_bytes(body[key])
            if payload_bytes:
                return payload_bytes

    metadata = body.get("documentMetadata")
    if isinstance(metadata, dict):
        for inline_key in _INLINE_METADATA_KEYS:
            inline_payload = metadata.get(inline_key)
            if inline_payload:
                payload_bytes = _coerce_bytes(inline_payload)
                if payload_bytes:
                    return payload_bytes
    return None


class ContentResolver(Protocol):
    """Resolves raw document content for analysis."""

    def fetch(self, descriptor: DocumentDescriptor) -> Optional[bytes]:
        ...


class InlineDocumentContentResolver:
    """Retrieves base64-encoded or inline binary payloads from the message body."""

    INLINE_KEYS = _INLINE_PAYLOAD_KEYS

    def fetch(self, descriptor: DocumentDescriptor) -> Optional[bytes]:
        body = descriptor.body
        return _extract_inline_bytes(body)


class LayoutAnalyser(Protocol):
    """Produces a :class:`DocumentProfile` for routing."""

    def analyse(
        self, descriptor: DocumentDescriptor, content: Optional[bytes] = None
    ) -> DocumentProfile:
        ...


class LayoutModelClient(Protocol):
    """Interface for advanced CV/deep-learning based layout analysers."""

    def infer_layout(
        self, descriptor: DocumentDescriptor, content: Optional[bytes] = None
    ) -> Sequence[PageMetrics]:
        ...


class LayoutModelType(str, Enum):
    """Enumerates supported deep-learning models for layout analysis."""

    LAYOUTLM_V3 = "layoutlm_v3"
    DOCFORMER = "docformer"
    TABLE_DETR = "table_detr"
    FORM_CLASSIFIER = "form_classifier"


@dataclass
class RouterConfig:
    """Configuration for :class:`DocumentRouter`."""

    mode: RoutingMode = RoutingMode.HYBRID
    request_override_flag: str = "parser_override"
    category_thresholds: Dict[str, int] = field(default_factory=dict)
    default_strategy_map: Dict[str, Dict[str, object]] = field(default_factory=dict)
    fallback_strategy: Optional[Dict[str, object]] = None
    static_strategy: Optional[Dict[str, object]] = None
    scanned_page_ratio_threshold: float = 0.5
    table_page_ratio_threshold: float = 0.3
    form_page_ratio_threshold: float = 0.25
    short_form_min_text_density: float = 0.55

    def __post_init__(self) -> None:
        if isinstance(self.mode, str):
            self.mode = RoutingMode(self.mode)

        default_map: Dict[DocumentCategory, StrategyConfig] = {}
        for key, value in (self.default_strategy_map or {}).items():
            try:
                category = DocumentCategory(key)
            except ValueError:
                category = DocumentCategory.UNKNOWN
            default_map[category] = (
                value if isinstance(value, StrategyConfig) else StrategyConfig.from_mapping(value)  # type: ignore[arg-type]
            )

        if isinstance(self.fallback_strategy, dict):
            self.fallback_strategy = StrategyConfig.from_mapping(self.fallback_strategy)
        elif isinstance(self.fallback_strategy, StrategyConfig):
            self.fallback_strategy = self.fallback_strategy
        else:
            self.fallback_strategy = StrategyConfig(name="fallback_non_azure")

        if DocumentCategory.UNKNOWN not in default_map:
            default_map[DocumentCategory.UNKNOWN] = self.fallback_strategy  # type: ignore[assignment]

        self.default_strategy_map = default_map

        if self.static_strategy and not isinstance(self.static_strategy, StrategyConfig):
            self.static_strategy = StrategyConfig.from_mapping(self.static_strategy)

        thresholds = self.category_thresholds or {}
        self.long_form_threshold = int(thresholds.get("long_form_threshold", 100))
        self.short_form_threshold = int(thresholds.get("short_form_threshold", 15))
        self.short_form_max_pages = _safe_int(thresholds.get("short_form_max_pages"))
        self.long_form_max_pages = _safe_int(thresholds.get("long_form_max_pages"))
        self.table_heavy_max_pages = _safe_int(thresholds.get("table_heavy_max_pages"))
        self.form_max_pages = _safe_int(thresholds.get("form_max_pages"))

    def strategy_for_category(self, category: DocumentCategory) -> StrategyConfig:
        return self.default_strategy_map.get(category) or self.default_strategy_map[DocumentCategory.UNKNOWN]


class HeuristicLayoutAnalyser:
    """Builds a :class:`DocumentProfile` using embedded metadata heuristics."""

    def analyse(
        self, descriptor: DocumentDescriptor, content: Optional[bytes] = None
    ) -> DocumentProfile:
        metadata = descriptor.body.get("documentMetadata", {}) if isinstance(descriptor.body, dict) else {}
        layout = metadata.get("layout") or {}

        page_metrics: List[PageMetrics] = []
        pages_payload = layout.get("pages")
        if isinstance(pages_payload, list):
            for idx, page in enumerate(pages_payload):
                page_metrics.append(_page_metrics_from_payload(idx, page))

        if not page_metrics:
            inferred_pages = max(_infer_page_count(descriptor.body) or 0, 1)
            text_density = _safe_float(layout.get("textDensity"), 0.5)
            image_density = _safe_float(layout.get("imageDensity"), 1 - text_density)
            table_density = _safe_float(layout.get("tableDensity"), 0.0)
            for idx in range(inferred_pages):
                page_metrics.append(
                    PageMetrics(
                        index=idx,
                        text_density=text_density,
                        image_density=image_density,
                        table_density=table_density,
                    )
                )

        return _build_profile(descriptor, page_metrics)


class ModelBackedLayoutAnalyser:
    """Delegates page analysis to an external model with heuristic fallback."""

    def __init__(self, model_client: LayoutModelClient, fallback: Optional[LayoutAnalyser] = None) -> None:
        self.model_client = model_client
        self.fallback = fallback or HeuristicLayoutAnalyser()

    def analyse(
        self, descriptor: DocumentDescriptor, content: Optional[bytes] = None
    ) -> DocumentProfile:
        try:
            metrics = list(self.model_client.infer_layout(descriptor, content))
            if metrics:
                return _build_profile(descriptor, metrics)
        except Exception:  # pragma: no cover - defensive logging
            logger.exception("Layout model inference failed for document %s", descriptor.object_key)
        return self.fallback.analyse(descriptor, content)


class _EmailHTMLMetricsParser(HTMLParser):
    """Accumulates layout artefacts from HTML content."""

    def __init__(self) -> None:
        super().__init__()
        self._text_fragments: List[str] = []
        self.table_count = 0
        self.checkbox_count = 0
        self.radio_button_count = 0
        self.image_count = 0

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag == "table":
            self.table_count += 1
        elif tag == "img":
            self.image_count += 1
        elif tag == "input":
            attrs_dict = {key.lower(): (value or "").lower() for key, value in attrs}
            input_type = attrs_dict.get("type", "")
            if input_type == "checkbox":
                self.checkbox_count += 1
            elif input_type == "radio":
                self.radio_button_count += 1

    def handle_data(self, data: str) -> None:
        if data:
            self._text_fragments.append(data)

    def build_metrics(self, index: int) -> PageMetrics:
        text = "".join(self._text_fragments)
        char_count = len(text.strip())
        text_density = min(char_count / 4000.0, 1.0)
        image_density = min(self.image_count * 0.1, 1.0)
        table_density = min(self.table_count * 0.25, 1.0)
        return PageMetrics(
            index=index,
            text_density=text_density,
            image_density=image_density,
            table_density=table_density,
            char_count=char_count,
            table_count=self.table_count,
            image_count=self.image_count,
            checkbox_count=self.checkbox_count,
            radio_button_count=self.radio_button_count,
        )


class PyMuPDFLayoutAnalyser:
    """Uses PyMuPDF to perform rich page-level analysis of PDFs and emails."""

    PDF_MIME_TYPES = {
        "application/pdf",
        "application/x-pdf",
        "application/acrobat",
    }
    EMAIL_MIME_TYPES = {
        "message/rfc822",
        "application/vnd.ms-outlook",
    }

    def __init__(
        self,
        content_resolvers: Optional[Sequence[ContentResolver]] = None,
        fallback: Optional[LayoutAnalyser] = None,
    ) -> None:
        self.content_resolvers = list(content_resolvers) if content_resolvers else [InlineDocumentContentResolver()]
        self.fallback = fallback or HeuristicLayoutAnalyser()

    def analyse(
        self, descriptor: DocumentDescriptor, content: Optional[bytes] = None
    ) -> DocumentProfile:
        payload = content or self._resolve_content(descriptor)
        if not payload:
            return self.fallback.analyse(descriptor, content)

        mime_type = (descriptor.mime_type or "").lower()
        object_key = (descriptor.object_key or "").lower()

        if mime_type in self.PDF_MIME_TYPES or object_key.endswith(".pdf"):
            pdf_profile = self._analyse_pdf(descriptor, payload)
            if pdf_profile:
                return pdf_profile
        elif mime_type in self.EMAIL_MIME_TYPES or object_key.endswith((".eml", ".msg")):
            email_profile = self._analyse_email(descriptor, payload)
            if email_profile:
                return email_profile

        return self.fallback.analyse(descriptor, payload)

    def _resolve_content(self, descriptor: DocumentDescriptor) -> Optional[bytes]:
        for resolver in self.content_resolvers:
            try:
                content = resolver.fetch(descriptor)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "PyMuPDF content resolver %s failed for %s",
                    resolver.__class__.__name__,
                    descriptor.object_key,
                )
                continue
            if content:
                return content
        return None

    def _analyse_pdf(
        self, descriptor: DocumentDescriptor, payload: bytes
    ) -> Optional[DocumentProfile]:
        if fitz is None:
            logger.warning("PyMuPDF is not available; falling back to heuristic layout analyser")
            return None

        try:
            document = fitz.open(stream=payload, filetype="pdf")
        except Exception:
            logger.exception("Failed to open PDF document %s with PyMuPDF", descriptor.object_key)
            return None

        page_metrics: List[PageMetrics] = []
        try:
            for page_index in range(document.page_count):
                page = document.load_page(page_index)
                page_metrics.append(self._metrics_from_pdf_page(page, page_index))
        finally:
            document.close()

        return _build_profile(descriptor, page_metrics)

    def _metrics_from_pdf_page(self, page, index: int) -> PageMetrics:
        page_rect = page.rect
        page_area = max(page_rect.width * page_rect.height, 1.0)

        try:
            text_dict = page.get_text("dict")
        except Exception:
            text_dict = {"blocks": []}

        text_area = 0.0
        image_area = 0.0
        char_count = 0
        image_count = 0

        for block in text_dict.get("blocks", []):
            bbox = block.get("bbox")
            block_area = _rect_area(bbox)
            block_type = block.get("type")

            if block_type == 0:  # text block
                text_area += block_area
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        char_count += len(span.get("text", ""))
            elif block_type == 1:  # image block
                image_area += block_area
                image_count += 1

        table_area = 0.0
        table_count = 0
        try:
            table_search = page.find_tables()
        except Exception:
            table_search = None
        if table_search and getattr(table_search, "tables", None):
            for table in table_search.tables:
                table_area += _rect_area(getattr(table, "bbox", None))
            table_count = len(table_search.tables)

        checkbox_count = 0
        radio_button_count = 0
        try:
            widgets = page.widgets()
        except Exception:
            widgets = None
        if widgets:
            for widget in widgets:
                field_type = getattr(widget, "field_type", None)
                field_type_string = str(getattr(widget, "field_type_string", "")).lower()
                if field_type in (PDF_WIDGET_TYPE_CHECKBOX,) or "checkbox" in field_type_string:
                    checkbox_count += 1
                elif field_type in (PDF_WIDGET_TYPE_RADIOBUTTON,) or "radio" in field_type_string:
                    radio_button_count += 1

        text_density = min(text_area / page_area, 1.0)
        image_density = min(image_area / page_area, 1.0)
        table_density = min(table_area / page_area, 1.0)

        return PageMetrics(
            index=index,
            text_density=text_density,
            image_density=image_density,
            table_density=table_density,
            char_count=char_count,
            table_count=table_count,
            image_count=image_count,
            checkbox_count=checkbox_count,
            radio_button_count=radio_button_count,
        )

    def _analyse_email(
        self, descriptor: DocumentDescriptor, payload: bytes
    ) -> Optional[DocumentProfile]:
        try:
            email_message = message_from_bytes(payload)
        except Exception:
            logger.exception("Unable to parse email message for %s", descriptor.object_key)
            return None

        page_metrics: List[PageMetrics] = []
        part_index = 0

        if isinstance(email_message, Message):
            for part in email_message.walk():
                if part.is_multipart():
                    continue
                content_type = part.get_content_type()
                data = part.get_payload(decode=True) or b""
                if content_type in ("text/html", "application/xhtml+xml"):
                    parser = _EmailHTMLMetricsParser()
                    try:
                        parser.feed(data.decode(part.get_content_charset() or "utf-8", errors="ignore"))
                    finally:
                        parser.close()
                    page_metrics.append(parser.build_metrics(part_index))
                    part_index += 1
                elif content_type.startswith("text/"):
                    text = data.decode(part.get_content_charset() or "utf-8", errors="ignore")
                    metrics = self._metrics_from_plain_text(text, part_index)
                    page_metrics.append(metrics)
                    part_index += 1

        if not page_metrics:
            fallback_text = payload.decode("utf-8", errors="ignore")
            page_metrics.append(self._metrics_from_plain_text(fallback_text, 0))

        return _build_profile(descriptor, page_metrics)

    def _metrics_from_plain_text(self, text: str, index: int) -> PageMetrics:
        char_count = len(text.strip())
        text_density = min(char_count / 3000.0, 1.0)
        return PageMetrics(
            index=index,
            text_density=text_density,
            image_density=0.05,
            table_density=0.0,
            char_count=char_count,
            table_count=0,
            image_count=0,
            checkbox_count=0,
            radio_button_count=0,
        )

class RequestsLayoutModelClient:
    """Simple HTTP client to call an external layout analysis service."""

    def __init__(
        self,
        endpoint: str,
        api_key: Optional[str] = None,
        timeout_seconds: int = 30,
        model_type: Optional[LayoutModelType] = None,
    ) -> None:
        self.endpoint = endpoint
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.model_type = LayoutModelType(model_type) if model_type else None

    def infer_layout(
        self, descriptor: DocumentDescriptor, content: Optional[bytes] = None
    ) -> Sequence[PageMetrics]:
        payload = {
            "object_key": descriptor.object_key,
            "bucket": descriptor.bucket,
            "mime_type": descriptor.mime_type,
            "page_count": _infer_page_count(descriptor.body),
            "metadata": descriptor.body.get("documentMetadata"),
        }

        if self.model_type:
            payload["model_type"] = self.model_type.value

        if content:
            payload["document"] = base64.b64encode(content).decode("ascii")

        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        request_obj = urllib_request.Request(self.endpoint, data=data, headers=headers)
        try:
            with urllib_request.urlopen(request_obj, timeout=self.timeout_seconds) as response:
                response_payload = json.loads(response.read().decode("utf-8"))
        except URLError as exc:  # pragma: no cover - network call
            raise RuntimeError(f"Failed to call layout model endpoint: {exc}") from exc

        pages = []
        for idx, page in enumerate(response_payload.get("pages", [])):
            pages.append(_page_metrics_from_payload(idx, page))
        return pages


class DocumentRouter:
    """Coordinates document analysis and parser strategy selection."""

    def __init__(
        self,
        config: RouterConfig,
        layout_analyser: LayoutAnalyser,
        content_resolvers: Optional[Sequence[ContentResolver]] = None,
    ) -> None:
        self.config = config
        self.layout_analyser = layout_analyser
        self.content_resolvers = (
            list(content_resolvers) if content_resolvers else [InlineDocumentContentResolver()]
        )

    def route(self, body: dict, object_key: str, overrides: OverrideSet) -> DocumentAnalysis:
        descriptor = self._build_descriptor(body, object_key)
        content = self._resolve_content(descriptor)
        profile = self.layout_analyser.analyse(descriptor, content)
        category = self._categorise(profile)
        strategy, applied = self._resolve_strategy(profile, descriptor, overrides, category)

        return DocumentAnalysis(
            object_key=descriptor.object_key,
            mime_type=profile.mime_type,
            page_count=profile.page_count,
            category=category,
            strategy=strategy,
            overrides_applied=applied,
            request_override=descriptor.request_override,
            average_text_density=profile.average_text_density,
            average_image_density=profile.average_image_density,
            table_page_ratio=profile.table_page_ratio,
            scanned_page_ratio=profile.scanned_page_ratio,
            checkbox_page_ratio=profile.checkbox_page_ratio,
            radio_button_page_ratio=profile.radio_button_page_ratio,
            form_page_ratio=profile.form_page_ratio,
            total_tables=profile.total_tables,
            total_checkboxes=profile.total_checkboxes,
            total_radio_buttons=profile.total_radio_buttons,
            pages=profile.pages,
            raw_metadata=body,
        )

    def _resolve_content(self, descriptor: DocumentDescriptor) -> Optional[bytes]:
        for resolver in self.content_resolvers:
            try:
                content = resolver.fetch(descriptor)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "Content resolver %s failed for document %s",
                    resolver.__class__.__name__,
                    descriptor.object_key,
                )
                continue
            if content:
                return content
        return None

    def _resolve_strategy(
        self,
        profile: DocumentProfile,
        descriptor: DocumentDescriptor,
        overrides: OverrideSet,
        category: DocumentCategory,
    ) -> (ParserStrategy, List[str]):
        strategy, applied = self._apply_overrides(descriptor, overrides)
        if strategy:
            return strategy, applied

        if self.config.mode == RoutingMode.STATIC and self.config.static_strategy:
            applied.append("static_config")
            static_cfg: StrategyConfig = (
                self.config.static_strategy
                if isinstance(self.config.static_strategy, StrategyConfig)
                else StrategyConfig.from_mapping(self.config.static_strategy or {})
            )
            return (
                ParserStrategy(
                    name=static_cfg.name,
                    reason="config_static",
                    model=static_cfg.model,
                    max_pages=static_cfg.max_pages,
                ),
                applied,
            )

        strategy, applied = self._determine_strategy(profile, category, applied)
        return strategy, applied

    def _build_descriptor(self, body: dict, object_key: str) -> DocumentDescriptor:
        bucket = None
        if isinstance(body, dict):
            bucket = body.get("s3", {}).get("bucket", {}).get("name")
        mime_type = _sniff_mime_type(object_key, body)
        request_override = None
        if isinstance(body, dict):
            routing_block = body.get("routing") or body.get("overrides")
            if self.config.request_override_flag in body:
                request_override = body.get(self.config.request_override_flag)
            elif isinstance(routing_block, dict):
                request_override = routing_block.get(self.config.request_override_flag)
        return DocumentDescriptor(
            object_key=object_key,
            bucket=bucket,
            body=body,
            mime_type=mime_type,
            request_override=request_override,
        )

    def _categorise(self, profile: DocumentProfile) -> DocumentCategory:
        if profile.page_count == 0:
            return DocumentCategory.UNKNOWN

        if profile.scanned_page_ratio >= self.config.scanned_page_ratio_threshold:
            return DocumentCategory.SCANNED

        if profile.table_page_ratio >= self.config.table_page_ratio_threshold:
            return DocumentCategory.TABLE_HEAVY

        if profile.form_page_ratio >= self.config.form_page_ratio_threshold:
            return DocumentCategory.FORM_HEAVY

        if profile.page_count >= self.config.long_form_threshold:
            return DocumentCategory.LONG_FORM

        if (
            profile.page_count <= self.config.short_form_threshold
            and profile.average_text_density >= self.config.short_form_min_text_density
        ):
            return DocumentCategory.SHORT_FORM

        return DocumentCategory.UNKNOWN

    def _apply_overrides(
        self, descriptor: DocumentDescriptor, overrides: OverrideSet
    ) -> (Optional[ParserStrategy], List[str]):
        applied: List[str] = []
        if descriptor.request_override:
            applied.append("request")
            return (
                ParserStrategy(
                    name=descriptor.request_override,
                    reason="request_override",
                ),
                applied,
            )

        for override in overrides.pattern_overrides:
            if override.pattern.search(descriptor.object_key or ""):
                applied.append(f"pattern:{override.pattern.pattern}")
                return (
                    ParserStrategy(
                        name=override.strategy.name,
                        reason="config_pattern_override",
                        model=override.strategy.model,
                        max_pages=override.strategy.max_pages,
                    ),
                    applied,
                )

        return None, applied

    def _determine_strategy(
        self, profile: DocumentProfile, category: DocumentCategory, applied: List[str]
    ) -> (ParserStrategy, List[str]):
        threshold = {
            DocumentCategory.SHORT_FORM: self.config.short_form_max_pages,
            DocumentCategory.LONG_FORM: self.config.long_form_max_pages,
            DocumentCategory.TABLE_HEAVY: self.config.table_heavy_max_pages,
            DocumentCategory.FORM_HEAVY: self.config.form_max_pages,
        }.get(category)

        if threshold and profile.page_count > threshold:
            applied.append("threshold_redirect")
            fallback = (
                self.config.fallback_strategy
                if isinstance(self.config.fallback_strategy, StrategyConfig)
                else StrategyConfig.from_mapping(self.config.fallback_strategy or {})
            )
            return (
                ParserStrategy(
                    name=fallback.name,
                    reason="page_threshold_exceeded",
                    model=fallback.model,
                    max_pages=threshold,
                ),
                applied,
            )

        applied.append("category_default")
        default_entry = self.config.strategy_for_category(category)
        return (
            ParserStrategy(
                name=default_entry.name,
                reason="category_default",
                model=default_entry.model,
                max_pages=default_entry.max_pages,
            ),
            applied,
        )


def _build_profile(descriptor: DocumentDescriptor, page_metrics: Sequence[PageMetrics]) -> DocumentProfile:
    page_count = len(page_metrics)
    if page_count == 0:
        page_count = _infer_page_count(descriptor.body) or 0

    def _mean(values: Iterable[float]) -> float:
        vals = list(values)
        if not vals:
            return 0.0
        return statistics.fmean(vals)

    average_text_density = _mean(page.text_density for page in page_metrics)
    average_image_density = _mean(page.image_density for page in page_metrics)
    table_page_ratio = (
        sum(
            1
            for page in page_metrics
            if page.table_density >= 0.5 or page.table_count > 0
        )
        / len(page_metrics)
        if page_metrics
        else 0.0
    )
    scanned_page_ratio = (
        sum(1 for page in page_metrics if page.image_density >= 0.6 or page.image_count > 2)
        / len(page_metrics)
        if page_metrics
        else 0.0
    )
    checkbox_page_ratio = (
        sum(1 for page in page_metrics if page.checkbox_count > 0)
        / len(page_metrics)
        if page_metrics
        else 0.0
    )
    radio_button_page_ratio = (
        sum(1 for page in page_metrics if page.radio_button_count > 0)
        / len(page_metrics)
        if page_metrics
        else 0.0
    )
    form_page_ratio = (
        sum(
            1
            for page in page_metrics
            if page.checkbox_count > 0 or page.radio_button_count > 0
        )
        / len(page_metrics)
        if page_metrics
        else 0.0
    )
    total_tables = sum(page.table_count for page in page_metrics)
    total_checkboxes = sum(page.checkbox_count for page in page_metrics)
    total_radio_buttons = sum(page.radio_button_count for page in page_metrics)

    return DocumentProfile(
        object_key=descriptor.object_key,
        bucket=descriptor.bucket,
        mime_type=descriptor.mime_type,
        page_count=page_count,
        pages=page_metrics,
        average_text_density=average_text_density,
        average_image_density=average_image_density,
        table_page_ratio=table_page_ratio,
        scanned_page_ratio=scanned_page_ratio,
        checkbox_page_ratio=checkbox_page_ratio,
        radio_button_page_ratio=radio_button_page_ratio,
        form_page_ratio=form_page_ratio,
        total_tables=total_tables,
        total_checkboxes=total_checkboxes,
        total_radio_buttons=total_radio_buttons,
    )


def _page_metrics_from_payload(idx: int, payload: dict) -> PageMetrics:
    text_density = _safe_float(payload.get("textDensity"), 0.5)
    if "text_density" in payload:
        text_density = _safe_float(payload.get("text_density"), text_density)

    image_density = _safe_float(payload.get("imageDensity"), 1 - text_density)
    if "image_density" in payload:
        image_density = _safe_float(payload.get("image_density"), image_density)

    table_density = _safe_float(payload.get("tableDensity"), payload.get("table_density"), 0.0)

    char_count = payload.get("charCount") or payload.get("char_count")
    if char_count is not None:
        try:
            char_count = int(char_count)
        except (TypeError, ValueError):
            char_count = None

    table_count = _safe_int(payload.get("tableCount") or payload.get("table_count") or 0) or 0
    image_count = _safe_int(payload.get("imageCount") or payload.get("image_count") or 0) or 0
    checkbox_count = _safe_int(
        payload.get("checkboxCount") or payload.get("checkbox_count") or 0
    ) or 0
    radio_button_count = _safe_int(
        payload.get("radioButtonCount") or payload.get("radio_button_count") or 0
    ) or 0

    return PageMetrics(
        index=int(payload.get("index", idx)),
        text_density=text_density,
        image_density=image_density,
        table_density=table_density,
        char_count=char_count,
        table_count=table_count,
        image_count=image_count,
        checkbox_count=checkbox_count,
        radio_button_count=radio_button_count,
    )


def _infer_page_count(body: dict) -> Optional[int]:
    if not isinstance(body, dict):
        return None

    metadata = body.get("documentMetadata", {})
    if "pageCount" in metadata:
        try:
            return int(metadata.get("pageCount"))
        except (TypeError, ValueError):
            return None

    layout = metadata.get("layout", {})
    if "pages" in layout and isinstance(layout["pages"], list):
        return len(layout["pages"])

    page_count = body.get("page_count") or body.get("pageCount")
    if page_count is not None:
        try:
            return int(page_count)
        except (TypeError, ValueError):
            return None

    return None


def _sniff_mime_type(object_key: str, body: dict) -> str:
    metadata = body.get("documentMetadata", {}) if isinstance(body, dict) else {}
    mime_type = metadata.get("contentType") or metadata.get("mimeType")
    if mime_type:
        return str(mime_type)

    inline_bytes = _extract_inline_bytes(body)
    if inline_bytes:
        detected = _detect_mime_from_bytes(inline_bytes)
        if detected:
            return detected

    guessed, _ = mimetypes.guess_type(object_key)
    return guessed or "application/octet-stream"


def _detect_mime_from_bytes(data: bytes) -> Optional[str]:
    if not data:
        return None

    header = data[:8]
    if header.startswith(b"%PDF-"):
        return "application/pdf"

    if header.startswith(b"\xD0\xCF\x11\xE0"):
        return "application/msword"

    if header.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                names = archive.namelist()
        except (zipfile.BadZipFile, RuntimeError):
            return "application/zip"

        name_set = {name.lower() for name in names}
        if any(name.startswith("word/") for name in name_set):
            return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        if any(name.startswith("ppt/") for name in name_set):
            return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        if any(name.startswith("xl/") for name in name_set):
            return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        return "application/zip"

    try:
        snippet = data[:2048].decode("utf-8", errors="ignore").strip()
    except Exception:
        snippet = ""

    lowered = snippet.lower()
    if lowered.startswith("<!doctype html") or lowered.startswith("<html") or "<html" in lowered[:200]:
        return "text/html"
    if lowered.startswith("<?xml"):
        return "application/xml"
    if lowered.startswith("from:") or lowered.startswith("received:"):
        return "message/rfc822"

    sample = data[:128]
    if sample:
        ascii_like = sum(1 for byte in sample if 32 <= byte <= 126 or byte in {9, 10, 13}) / len(sample)
        if ascii_like > 0.9:
            return "text/plain"

    return None


def _safe_float(*values: object, default: float = 0.0) -> float:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return default


def _safe_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _rect_area(bbox: Optional[Sequence[float]]) -> float:
    if not bbox or len(bbox) != 4:
        return 0.0
    x0, y0, x1, y1 = bbox
    width = max(float(x1) - float(x0), 0.0)
    height = max(float(y1) - float(y0), 0.0)
    return max(width * height, 0.0)

__all__ = [
    "RoutingMode",
    "DocumentCategory",
    "StrategyConfig",
    "PatternOverride",
    "OverrideSet",
    "ParserStrategy",
    "PageMetrics",
    "DocumentProfile",
    "DocumentAnalysis",
    "DocumentDescriptor",
    "ContentResolver",
    "InlineDocumentContentResolver",
    "LayoutAnalyser",
    "LayoutModelClient",
    "LayoutModelType",
    "HeuristicLayoutAnalyser",
    "ModelBackedLayoutAnalyser",
    "PyMuPDFLayoutAnalyser",
    "RequestsLayoutModelClient",
    "RouterConfig",
    "DocumentRouter",
    "_EmailHTMLMetricsParser",
    "_build_profile",
    "_sniff_mime_type",
]
