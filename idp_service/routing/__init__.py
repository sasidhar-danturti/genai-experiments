"""Document routing package for ingestion workflows.

This package now re-exports the standalone ``idp_router`` library so that
existing imports continue to function while allowing the router to be
packaged independently.
"""

from idp_router import (  # noqa: F401
    ContentResolver,
    DocumentAnalysis,
    DocumentCategory,
    DocumentDescriptor,
    DocumentProfile,
    DocumentRouter,
    HeuristicLayoutAnalyser,
    InlineDocumentContentResolver,
    LayoutAnalyser,
    LayoutModelClient,
    LayoutModelType,
    ModelBackedLayoutAnalyser,
    OverrideSet,
    PageMetrics,
    ParserStrategy,
    PatternOverride,
    PyMuPDFLayoutAnalyser,
    RequestsLayoutModelClient,
    RouterConfig,
    RoutingMode,
    StrategyConfig,
    HuggingFaceLayoutModelClient,
)

__all__ = [
    "ContentResolver",
    "DocumentAnalysis",
    "DocumentCategory",
    "DocumentDescriptor",
    "DocumentProfile",
    "DocumentRouter",
    "HeuristicLayoutAnalyser",
    "InlineDocumentContentResolver",
    "LayoutAnalyser",
    "LayoutModelClient",
    "LayoutModelType",
    "ModelBackedLayoutAnalyser",
    "OverrideSet",
    "PageMetrics",
    "ParserStrategy",
    "PatternOverride",
    "PyMuPDFLayoutAnalyser",
    "RequestsLayoutModelClient",
    "RouterConfig",
    "RoutingMode",
    "StrategyConfig",
    "HuggingFaceLayoutModelClient",
]
