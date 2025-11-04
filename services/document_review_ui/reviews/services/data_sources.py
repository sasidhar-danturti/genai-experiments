"""Data source abstractions for fetching canonical review artefacts."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Protocol

from django.core.exceptions import ImproperlyConfigured


@dataclass(frozen=True)
class ReviewDocument:
    """Aggregate of the canonical, standardised, and insight payloads."""

    document_id: str
    canonical: Dict[str, Any]
    standardized: Optional[Dict[str, Any]] = None
    insights: Optional[Dict[str, Any]] = None
    job_id: Optional[str] = None
    trigger: Optional[str] = None


class DocumentDataSource(Protocol):
    """Interface for retrieving documents that require review."""

    def fetch(self, document_id: str) -> ReviewDocument:
        """Return a document payload by identifier."""

    def iter_pending(self, *, limit: int = 100) -> Iterable[ReviewDocument]:
        """Yield documents awaiting review."""


class DatabricksDataSource:
    """Load canonical documents from Databricks SQL warehouses."""

    def __init__(self) -> None:
        try:
            from services.document_processing_api.databricks_sql_client import DatabricksSQLClient
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ImproperlyConfigured(
                "Databricks dependencies are not installed. Install databricks-sql-connector to enable this source."
            ) from exc

        host = os.environ.get("DOCUMENT_REVIEW_DATABRICKS_HOST")
        token = os.environ.get("DOCUMENT_REVIEW_DATABRICKS_TOKEN")
        endpoint = os.environ.get("DOCUMENT_REVIEW_DATABRICKS_ENDPOINT")
        catalog = os.environ.get("DOCUMENT_REVIEW_DATABRICKS_CATALOG")
        schema = os.environ.get("DOCUMENT_REVIEW_DATABRICKS_SCHEMA")
        table = os.environ.get("DOCUMENT_REVIEW_DATABRICKS_TABLE")
        if not all([host, token, endpoint, catalog, schema, table]):
            raise ImproperlyConfigured(
                "Databricks data source requires host, token, endpoint, catalog, schema, and table environment variables"
            )
        self._client = DatabricksSQLClient(host, token, endpoint, catalog, schema, table)

    def fetch(self, document_id: str) -> ReviewDocument:
        documents = list(self.iter_pending(limit=1_000))
        for document in documents:
            if document.document_id == document_id:
                return document
        raise LookupError(f"Document {document_id} not found in Databricks source")

    def iter_pending(self, *, limit: int = 100) -> Iterable[ReviewDocument]:
        job_id = os.environ.get("DOCUMENT_REVIEW_DATABRICKS_JOB_ID")
        if not job_id:
            raise ImproperlyConfigured("DOCUMENT_REVIEW_DATABRICKS_JOB_ID must be provided to pull pending documents")
        documents, _ = self._client.fetch_canonical_documents(job_id, page_size=limit)
        for payload in documents:
            document_id = payload.get("document_id")
            insights = payload.get("insights") or {}
            standardized = payload.get("standardized_output") or {}
            trigger = payload.get("review_trigger")
            yield ReviewDocument(
                document_id=document_id,
                canonical=payload,
                standardized=standardized,
                insights=insights,
                job_id=payload.get("job_id"),
                trigger=trigger,
            )


class RedshiftDataSource:
    """Load canonical documents from Amazon Redshift."""

    def __init__(self) -> None:
        try:
            import psycopg2
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ImproperlyConfigured(
                "psycopg2 is required to use the Redshift data source"
            ) from exc

        host = os.environ.get("DOCUMENT_REVIEW_REDSHIFT_HOST")
        port = os.environ.get("DOCUMENT_REVIEW_REDSHIFT_PORT", "5439")
        database = os.environ.get("DOCUMENT_REVIEW_REDSHIFT_DATABASE")
        user = os.environ.get("DOCUMENT_REVIEW_REDSHIFT_USER")
        password = os.environ.get("DOCUMENT_REVIEW_REDSHIFT_PASSWORD")
        table = os.environ.get("DOCUMENT_REVIEW_REDSHIFT_TABLE")
        if not all([host, database, user, password, table]):
            raise ImproperlyConfigured(
                "Redshift data source requires host, database, user, password, and table environment variables"
            )
        self._connection = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        self._table = table

    def fetch(self, document_id: str) -> ReviewDocument:
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"SELECT canonical_document, standardized_output, insights, job_id, review_trigger "
                f"FROM {self._table} WHERE document_id = %s",
                (document_id,),
            )
            row = cursor.fetchone()
            if not row:
                raise LookupError(f"Document {document_id} not found in Redshift source")
            canonical_payload = self._coerce_json(row[0])
            standardized_payload = self._coerce_json(row[1])
            insights_payload = self._coerce_json(row[2])
            return ReviewDocument(
                document_id=document_id,
                canonical=canonical_payload,
                standardized=standardized_payload,
                insights=insights_payload,
                job_id=row[3],
                trigger=row[4],
            )

    def iter_pending(self, *, limit: int = 100) -> Iterable[ReviewDocument]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"SELECT document_id, canonical_document, standardized_output, insights, job_id, review_trigger "
                f"FROM {self._table} WHERE review_required = TRUE ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
            for row in cursor.fetchall():
                document_id = row[0]
                canonical_payload = self._coerce_json(row[1])
                standardized_payload = self._coerce_json(row[2])
                insights_payload = self._coerce_json(row[3])
                yield ReviewDocument(
                    document_id=document_id,
                    canonical=canonical_payload,
                    standardized=standardized_payload,
                    insights=insights_payload,
                    job_id=row[4],
                    trigger=row[5],
                )

    @staticmethod
    def _coerce_json(value: Any) -> Dict[str, Any] | None:
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8")
        if isinstance(value, str):
            return json.loads(value)
        if isinstance(value, dict):
            return value
        return json.loads(json.dumps(value))


def data_source_from_env() -> DocumentDataSource:
    """Instantiate a data source based on environment configuration."""

    backend = os.environ.get("DOCUMENT_REVIEW_SOURCE", "databricks").lower()
    if backend == "databricks":
        return DatabricksDataSource()
    if backend == "redshift":
        return RedshiftDataSource()
    raise ImproperlyConfigured(f"Unsupported DOCUMENT_REVIEW_SOURCE '{backend}'")
