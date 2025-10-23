"""Sample integration client for the Document Processing API."""

from __future__ import annotations

import time
from typing import Any, Dict, Iterator, Optional
from urllib.parse import urljoin

try:  # pragma: no cover - optional dependency for documentation examples.
    import requests
except Exception:  # pragma: no cover
    requests = None


class DocumentProcessingClient:
    """Minimal convenience wrapper around the public API surface."""

    def __init__(self, base_url: str, api_key: Optional[str] = None, timeout: float = 10.0):
        if requests is None:  # pragma: no cover
            raise RuntimeError("The requests package is required to use DocumentProcessingClient")

        self._base_url = base_url.rstrip("/") + "/"
        self._session = requests.Session()
        self._timeout = timeout
        if api_key:
            self._session.headers.update({"Authorization": f"Bearer {api_key}"})
        self._session.headers.setdefault("Content-Type", "application/json")

    def submit_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = self._session.post(self._url("jobs"), json=payload, timeout=self._timeout)
        response.raise_for_status()
        return response.json()

    def get_status(self, job_id: str) -> Dict[str, Any]:
        response = self._session.get(self._url(f"jobs/{job_id}"), timeout=self._timeout)
        response.raise_for_status()
        return response.json()

    def iter_results(self, job_id: str, page_size: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if page_size is not None:
            params["page_size"] = page_size
        page_token: Optional[str] = None
        while True:
            if page_token:
                params["page_token"] = page_token
            response = self._session.get(
                self._url(f"jobs/{job_id}/results"), params=params, timeout=self._timeout
            )
            response.raise_for_status()
            payload = response.json()
            for document in payload.get("documents", []):
                yield document
            page_token = payload.get("next_page_token")
            if not page_token:
                break

    def wait_for_completion(
        self,
        job_id: str,
        *,
        poll_interval: float = 5.0,
        timeout: float = 600.0,
    ) -> Dict[str, Any]:
        """Block until the job reaches a terminal state."""

        deadline = time.time() + timeout
        while True:
            status_payload = self.get_status(job_id)
            status = status_payload["status"]
            if status in {"succeeded", "failed", "partially_succeeded", "cancelled"}:
                return status_payload
            if time.time() >= deadline:
                raise TimeoutError(f"Job {job_id} did not complete within {timeout} seconds")
            time.sleep(poll_interval)

    def _url(self, path: str) -> str:
        return urljoin(self._base_url, path)


__all__ = ["DocumentProcessingClient"]
