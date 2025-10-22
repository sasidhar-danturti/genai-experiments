"""Adapter implementations for canonical parser outputs."""

from .base import AdapterError, ParserAdapter
from .azure_document_intelligence import AzureDocumentIntelligenceAdapter
from .databricks_llm_image import DatabricksLLMImageAdapter
from .email_parser import EmailParserAdapter
from .multi_parser import MultiParserAdapter
from .pymupdf import PyMuPDFAdapter

__all__ = [
    "AdapterError",
    "ParserAdapter",
    "AzureDocumentIntelligenceAdapter",
    "DatabricksLLMImageAdapter",
    "EmailParserAdapter",
    "MultiParserAdapter",
    "PyMuPDFAdapter",
]
