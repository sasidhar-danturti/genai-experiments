"""Minimal Databricks SQL client for fetching canonical results."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

LOGGER = logging.getLogger(__name__)


class DatabricksSQLClient:
    """Wrapper around the Databricks SQL REST API."""

    def __init__(
        self,
        host: str,
        token: str,
        endpoint_id: str,
        catalog: str,
        schema: str,
        table: str,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        self._host = host.rstrip("/")
        self._token = token
        self._endpoint_id = endpoint_id
        self._catalog = catalog
        self._schema = schema
        self._table = table
        self._poll_interval = poll_interval_seconds

    def fetch_canonical_documents(
        self, job_id: str, *, page_size: int, page_token: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Return canonical payloads stored in the Databricks results table."""

        offset = int(page_token or 0)
        statement = self._build_statement()
        LOGGER.debug("Executing Databricks SQL statement for job %s", job_id)
        submission = self._post_json(
            "/api/2.0/sql/statements",
            {
                "warehouse_id": self._endpoint_id,
                "catalog": self._catalog,
                "schema": self._schema,
                "statement": statement,
                "parameters": [
                    {"name": "job_id", "value": {"stringValue": job_id}},
                    {"name": "limit", "value": {"longValue": page_size}},
                    {"name": "offset", "value": {"longValue": offset}},
                ],
            },
        )
        statement_id = submission["statement_id"]
        result = self._wait_for_statement(statement_id)
        rows = result.get("data_array", [])
        documents = []
        for row in rows:
            canonical_payload = row[0]
            if isinstance(canonical_payload, str):
                canonical_payload = json.loads(canonical_payload)
            documents.append(canonical_payload)
        has_more = result.get("next_chunk_internal_link_path") is not None
        next_token = str(offset + len(documents)) if has_more else None
        return documents, next_token

    def _build_statement(self) -> str:
        fqn = f"{self._catalog}.{self._schema}.{self._table}"
        return (
            f"SELECT canonical_document FROM {fqn} "
            "WHERE job_id = :job_id ORDER BY sequence LIMIT :limit OFFSET :offset"
        )

    def _wait_for_statement(self, statement_id: str) -> Dict[str, Any]:
        path = f"/api/2.0/sql/statements/{statement_id}"
        while True:
            payload = self._get_json(path)
            state = payload.get("status", {}).get("state")
            if state in {"SUCCEEDED", "FAILED", "CANCELED"}:
                if state != "SUCCEEDED":
                    raise RuntimeError(f"Databricks SQL statement {statement_id} ended with state {state}")
                result = payload.get("result") or {}
                if not result:
                    LOGGER.warning("Databricks SQL statement %s returned no result payload", statement_id)
                return result
            time.sleep(self._poll_interval)

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "User-Agent": "document-processing-api/1.0",
        }

    def _post_json(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        data = json.dumps(body).encode("utf-8")
        request = Request(self._url(path), data=data, headers=self._headers(), method="POST")
        with urlopen(request, timeout=30) as response:  # nosec B310
            payload = response.read().decode("utf-8")
            return json.loads(payload)

    def _get_json(self, path: str) -> Dict[str, Any]:
        request = Request(self._url(path), headers=self._headers(), method="GET")
        with urlopen(request, timeout=30) as response:  # nosec B310
            payload = response.read().decode("utf-8")
            return json.loads(payload)

    def _url(self, path: str) -> str:
        if path.startswith("http"):
            return path
        return f"https://{self._host}{path}"
