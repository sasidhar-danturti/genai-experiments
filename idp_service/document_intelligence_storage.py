"""Persistence helpers for document intelligence outputs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

try:  # pragma: no cover - pyspark may be unavailable during unit tests
    from pyspark.sql import DataFrame, SparkSession  # type: ignore
    from pyspark.sql.functions import col  # type: ignore
except ImportError:  # pragma: no cover - provide a shim so the module still imports
    SparkSession = Any  # type: ignore
    DataFrame = Any  # type: ignore

    def col(_name: str):  # type: ignore
        raise RuntimeError("pyspark is required to use DeltaDocumentResultStore")

from parsers.canonical_schema import CanonicalDocument
from .document_intelligence_workflow import DocumentResultStore

logger = logging.getLogger(__name__)


@dataclass
class DeltaDocumentResultStore(DocumentResultStore):
    """Persists canonical documents into Delta tables."""

    spark: SparkSession
    table_name: str

    def has_record(self, document_id: str, checksum: str) -> bool:
        try:
            df = self.spark.table(self.table_name)
        except Exception:
            logger.debug("Result table %s not found; treating as empty", self.table_name)
            return False

        return (
            df.filter((col("document_id") == document_id) & (col("checksum") == checksum))
            .limit(1)
            .count()
            > 0
        )

    def save(self, result: CanonicalDocument) -> None:
        record = result.to_record()
        df: DataFrame = self.spark.createDataFrame([record])
        (df.write.format("delta").mode("append").saveAsTable(self.table_name))


class InMemoryDocumentResultStore(DocumentResultStore):
    """A lightweight in-memory store for testing."""

    def __init__(self) -> None:
        self._records = {}

    def has_record(self, document_id: str, checksum: str) -> bool:
        return (document_id, checksum) in self._records

    def save(self, result: CanonicalDocument) -> None:
        self._records[(result.document_id, result.checksum)] = result
