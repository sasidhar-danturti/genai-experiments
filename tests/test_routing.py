import base64
import io
import re
import sys
import zipfile
from pathlib import Path

import pytest

# Allow tests to import the routing package without installing it.
sys.path.append(str(Path(__file__).resolve().parents[1]))

from idp_service.routing.router import (
    DocumentCategory,
    DocumentRouter,
    HeuristicLayoutAnalyser,
    OverrideSet,
    PageMetrics,
    PatternOverride,
    RouterConfig,
    RoutingMode,
    StrategyConfig,
    _EmailHTMLMetricsParser,
    _build_profile,
    DocumentDescriptor,
    _sniff_mime_type,
)


class _StubLayoutAnalyser:
    def __init__(self, factory):
        self._factory = factory

    def analyse(self, descriptor, content=None):
        metrics = list(self._factory())
        return _build_profile(descriptor, metrics)


def _default_router_config():
    default_strategy_map = {
        category.value: {"name": f"{category.value}_parser"}
        for category in DocumentCategory
    }
    return RouterConfig(
        mode=RoutingMode.HYBRID,
        category_thresholds={
            "short_form_threshold": 5,
            "long_form_threshold": 50,
            "short_form_max_pages": 4,
            "long_form_max_pages": 120,
            "table_heavy_max_pages": 3,
            "form_max_pages": 4,
        },
        default_strategy_map=default_strategy_map,
        fallback_strategy={"name": "fallback_strategy"},
    )


def test_email_html_metrics_parser_counts_elements():
    parser = _EmailHTMLMetricsParser()
    parser.feed(
        """
        <html>
            <body>
                <table><tr><td>row</td></tr></table>
                <input type="checkbox" />
                <input type="radio" />
                <img src="image.png" />
                Dense text content for metrics.
            </body>
        </html>
        """
    )
    metrics = parser.build_metrics(index=0)

    assert metrics.table_count == 1
    assert metrics.checkbox_count == 1
    assert metrics.radio_button_count == 1
    assert metrics.image_count == 1
    assert metrics.char_count > 0
    assert 0 < metrics.text_density <= 1


def test_heuristic_layout_analyser_generates_profile_without_page_details():
    analyser = HeuristicLayoutAnalyser()
    descriptor = DocumentDescriptor(
        object_key="memo.pdf",
        bucket="bucket",
        body={
            "documentMetadata": {
                "pageCount": 2,
                "layout": {"textDensity": 0.6, "imageDensity": 0.4},
            }
        },
        mime_type="application/pdf",
        request_override=None,
    )

    profile = analyser.analyse(descriptor)

    assert profile.page_count == 2
    assert len(profile.pages) == 2
    assert profile.average_text_density == pytest.approx(0.6)
    assert profile.total_tables == 0
    assert profile.table_page_ratio == 0


def test_document_router_prefers_request_override_over_patterns():
    config = _default_router_config()
    router = DocumentRouter(
        config=config,
        layout_analyser=_StubLayoutAnalyser(
            lambda: [
                PageMetrics(
                    index=0,
                    text_density=0.8,
                    image_density=0.1,
                    table_density=0.0,
                ),
                PageMetrics(
                    index=1,
                    text_density=0.7,
                    image_density=0.2,
                    table_density=0.0,
                ),
            ]
        ),
    )

    body = {
        "parser_override": "force_parser",
        "documentMetadata": {"contentType": "application/pdf", "pageCount": 2},
    }

    overrides = OverrideSet(
        pattern_overrides=[
            PatternOverride(
                pattern=re.compile("contract", re.IGNORECASE),
                strategy=StrategyConfig(name="pattern_parser"),
            )
        ]
    )

    analysis = router.route(body=body, object_key="contract.pdf", overrides=overrides)

    assert analysis.category == DocumentCategory.SHORT_FORM
    assert analysis.strategy.name == "force_parser"
    assert analysis.strategy.reason == "request_override"
    assert analysis.request_override == "force_parser"
    assert analysis.overrides_applied == ["request"]


def test_document_router_uses_fallback_when_table_pages_exceed_threshold():
    config = _default_router_config()
    router = DocumentRouter(
        config=config,
        layout_analyser=_StubLayoutAnalyser(
            lambda: [
                PageMetrics(
                    index=i,
                    text_density=0.2,
                    image_density=0.2,
                    table_density=0.9,
                    table_count=1,
                )
                for i in range(5)
            ]
        ),
    )

    body = {
        "documentMetadata": {"contentType": "application/pdf", "pageCount": 5},
    }

    analysis = router.route(body=body, object_key="invoice.pdf", overrides=OverrideSet())

    assert analysis.category == DocumentCategory.TABLE_HEAVY
    assert analysis.page_count == 5
    assert analysis.strategy.name == "fallback_strategy"
    assert analysis.strategy.reason == "page_threshold_exceeded"
    assert "threshold_redirect" in analysis.overrides_applied
    assert analysis.total_tables == 5
    assert analysis.table_page_ratio == 1


def test_sniff_mime_type_detects_pdf_from_inline_payload():
    payload = base64.b64encode(b"%PDF-1.7\n...").decode("ascii")
    body = {"documentBytes": payload}

    assert _sniff_mime_type("unknown.bin", body) == "application/pdf"


def test_sniff_mime_type_detects_docx_package():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr("[Content_Types].xml", "<Types></Types>")
        archive.writestr("word/document.xml", "<w:document></w:document>")
    body = {"payload": base64.b64encode(buffer.getvalue()).decode("ascii")}

    assert (
        _sniff_mime_type("submission.bin", body)
        == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )


def test_sniff_mime_type_detects_html_snippets():
    html_bytes = b"<!doctype html><html><body>test</body></html>"
    body = {"document_content": base64.b64encode(html_bytes).decode("ascii")}

    assert _sniff_mime_type("page.dat", body) == "text/html"
