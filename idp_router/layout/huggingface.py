"""Hugging Face powered layout detection for :mod:`idp_router`."""
from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

try:  # pragma: no cover - optional dependency
    from PIL import Image
except ImportError:  # pragma: no cover - optional dependency
    Image = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    import pytesseract
except ImportError:  # pragma: no cover - optional dependency
    pytesseract = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    import torch
except ImportError:  # pragma: no cover - optional dependency
    torch = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from transformers import AutoModelForTokenClassification, AutoProcessor
except ImportError:  # pragma: no cover - optional dependency
    AutoModelForTokenClassification = None  # type: ignore[assignment]
    AutoProcessor = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    import fitz
except ImportError:  # pragma: no cover - optional dependency
    fitz = None  # type: ignore[assignment]

from ..router import DocumentDescriptor, LayoutModelClient, PageMetrics


@dataclass
class _OCRTokens:
    """Container for OCR tokens extracted from a page image."""

    words: List[str]
    boxes: List[List[int]]
    pixel_boxes: List[Tuple[int, int, int, int]]

    @property
    def has_tokens(self) -> bool:
        return bool(self.words)


class HuggingFaceLayoutModelClient(LayoutModelClient):
    """Use Hugging Face LayoutLM models for layout inference.

    The client performs three steps when analysing a document:

    1. Convert the incoming document payload into per-page RGB images.
    2. Run OCR (via :mod:`pytesseract`) to obtain words and bounding boxes.
    3. Feed the OCR results into a LayoutLM model to classify each word into a
       semantic layout class. The final :class:`PageMetrics` values are derived
       from the aggregate area covered by each semantic class.

    The dependencies required for this client (``pytesseract``, ``Pillow``,
    ``torch`` and ``transformers``) are optional so that the base routing
    package can be installed without heavy ML libraries. When any of these
    dependencies are unavailable a clear :class:`RuntimeError` is raised during
    initialisation.
    """

    def __init__(
        self,
        model_id: str = "nielsr/layoutlmv3-finetuned-funsd",
        *,
        confidence_threshold: float = 0.6,
        device: Optional[str] = None,
        ocr_languages: str = "eng",
    ) -> None:
        self._ensure_dependencies()

        assert AutoProcessor is not None  # for type checkers
        assert AutoModelForTokenClassification is not None
        assert torch is not None

        self.model_id = model_id
        self.confidence_threshold = confidence_threshold
        self.ocr_languages = ocr_languages
        self.processor = AutoProcessor.from_pretrained(model_id)
        self.model = AutoModelForTokenClassification.from_pretrained(model_id)

        if device:
            self.device = device
        else:
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model.to(self.device)
        self.model.eval()

        config = getattr(self.model, "config", None)
        self.label_map = getattr(config, "id2label", {}) if config else {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def infer_layout(
        self, descriptor: DocumentDescriptor, content: Optional[bytes] = None
    ) -> Sequence[PageMetrics]:
        if content is None:
            raise ValueError("Document bytes are required for Hugging Face layout inference")

        page_images = list(self._extract_page_images(descriptor, content))
        metrics: List[PageMetrics] = []
        for index, image in enumerate(page_images):
            tokens = self._extract_tokens(image)
            metrics.append(self._metrics_from_tokens(image, tokens, index))
        return metrics

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _metrics_from_tokens(
        self, image: "Image.Image", tokens: _OCRTokens, page_index: int
    ) -> PageMetrics:
        if not tokens.has_tokens:
            return PageMetrics(index=page_index, text_density=0.0, image_density=0.0, table_density=0.0)

        assert torch is not None

        encoding = self.processor(
            images=image,
            text=tokens.words,
            boxes=tokens.boxes,
            return_tensors="pt",
            truncation=True,
        )
        encoding = {key: value.to(self.device) for key, value in encoding.items()}

        with torch.no_grad():
            outputs = self.model(**encoding)

        logits = outputs.logits
        probabilities = torch.softmax(logits, dim=-1)
        confidence, predicted_ids = torch.max(probabilities, dim=-1)

        confidence = confidence.squeeze(0)
        predicted_ids = predicted_ids.squeeze(0)

        if confidence.dim() == 0:
            confidence = confidence.unsqueeze(0)
            predicted_ids = predicted_ids.unsqueeze(0)

        page_area = max(image.width * image.height, 1)
        text_area = 0.0
        image_area = 0.0
        table_area = 0.0
        char_count = 0
        table_count = 0
        image_count = 0
        checkbox_count = 0
        radio_button_count = 0

        for idx, (label_id, score) in enumerate(zip(predicted_ids.tolist(), confidence.tolist())):
            if score < self.confidence_threshold:
                continue

            label = self.label_map.get(label_id, str(label_id))
            category = self._category_for_label(label)

            bbox = tokens.pixel_boxes[idx]
            area = max((bbox[2] - bbox[0]) * (bbox[3] - bbox[1]), 0)
            word = tokens.words[idx]

            if category == "text":
                text_area += area
                char_count += len(word)
            elif category == "table":
                table_area += area
                table_count += 1
            elif category == "image":
                image_area += area
                image_count += 1
            elif category == "checkbox":
                checkbox_count += 1
            elif category == "radio":
                radio_button_count += 1
            else:
                # Non recognised classes are ignored for density calculations but
                # can still influence character counts if they are textual.
                if label.upper().startswith(("B-", "I-")):
                    text_area += area
                    char_count += len(word)

        text_density = min(text_area / page_area, 1.0)
        image_density = min(image_area / page_area, 1.0)
        table_density = min(table_area / page_area, 1.0)

        return PageMetrics(
            index=page_index,
            text_density=text_density,
            image_density=image_density,
            table_density=table_density,
            char_count=char_count,
            table_count=table_count,
            image_count=image_count,
            checkbox_count=checkbox_count,
            radio_button_count=radio_button_count,
        )

    def _extract_tokens(self, image: "Image.Image") -> _OCRTokens:
        if pytesseract is None:
            raise RuntimeError(
                "pytesseract is required for HuggingFaceLayoutModelClient but is not installed"
            )

        ocr_result = pytesseract.image_to_data(
            image,
            lang=self.ocr_languages,
            output_type=pytesseract.Output.DICT,
        )

        width, height = image.size
        words: List[str] = []
        boxes: List[List[int]] = []
        pixel_boxes: List[Tuple[int, int, int, int]] = []

        for text, conf, left, top, box_width, box_height in zip(
            ocr_result.get("text", []),
            ocr_result.get("conf", []),
            ocr_result.get("left", []),
            ocr_result.get("top", []),
            ocr_result.get("width", []),
            ocr_result.get("height", []),
        ):
            if not text or text.isspace():
                continue
            try:
                confidence = float(conf)
            except (ValueError, TypeError):
                confidence = -1.0
            if confidence < 0:
                continue

            x0 = max(int(left), 0)
            y0 = max(int(top), 0)
            x1 = max(int(left + box_width), x0 + 1)
            y1 = max(int(top + box_height), y0 + 1)

            words.append(text)
            boxes.append(self._normalise_box(x0, y0, x1, y1, width, height))
            pixel_boxes.append((x0, y0, x1, y1))

        return _OCRTokens(words=words, boxes=boxes, pixel_boxes=pixel_boxes)

    def _extract_page_images(
        self, descriptor: DocumentDescriptor, content: bytes
    ) -> Iterable["Image.Image"]:
        if Image is None:
            raise RuntimeError(
                "Pillow is required for HuggingFaceLayoutModelClient but is not installed"
            )

        mime_type = (descriptor.mime_type or "").lower()
        if mime_type == "application/pdf":
            if fitz is None:
                raise RuntimeError(
                    "PyMuPDF is required to process PDF documents for HuggingFaceLayoutModelClient"
                )
            document = fitz.open(stream=content, filetype="pdf")
            try:
                for page_index in range(document.page_count):
                    page = document.load_page(page_index)
                    pixmap = page.get_pixmap(dpi=200)
                    image = Image.open(io.BytesIO(pixmap.tobytes("png")))
                    image.load()
                    yield image.convert("RGB")
            finally:
                document.close()
        elif mime_type.startswith("image/"):
            image = Image.open(io.BytesIO(content))
            image.load()
            yield image.convert("RGB")
        else:
            raise RuntimeError(
                f"Unsupported mime type {descriptor.mime_type!r} for HuggingFaceLayoutModelClient"
            )

    def _category_for_label(self, label: str) -> str:
        label_upper = label.upper()
        if "TABLE" in label_upper or "CELL" in label_upper:
            return "table"
        if "CHECKBOX" in label_upper or "CHECK" in label_upper:
            return "checkbox"
        if "RADIO" in label_upper:
            return "radio"
        if "FIGURE" in label_upper or "PICTURE" in label_upper or "IMAGE" in label_upper or "LOGO" in label_upper:
            return "image"
        if "HEADER" in label_upper or "QUESTION" in label_upper or "ANSWER" in label_upper:
            return "text"
        if label_upper in {"O", "TEXT", "PARA", "TITLE"}:
            return "text"
        if label_upper.startswith("B-") or label_upper.startswith("I-"):
            return "text"
        return "other"

    @staticmethod
    def _normalise_box(
        x0: int, y0: int, x1: int, y1: int, width: int, height: int
    ) -> List[int]:
        width = max(width, 1)
        height = max(height, 1)
        return [
            int(1000 * x0 / width),
            int(1000 * y0 / height),
            int(1000 * x1 / width),
            int(1000 * y1 / height),
        ]

    @staticmethod
    def _ensure_dependencies() -> None:
        missing = []
        if Image is None:
            missing.append("Pillow")
        if pytesseract is None:
            missing.append("pytesseract")
        if torch is None:
            missing.append("torch")
        if AutoProcessor is None or AutoModelForTokenClassification is None:
            missing.append("transformers")
        if missing:
            raise RuntimeError(
                "HuggingFaceLayoutModelClient requires optional dependencies: "
                + ", ".join(missing)
            )
