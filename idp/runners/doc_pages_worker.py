from __future__ import annotations

import json
from typing import Any

from idp.db_manager.spark_models import DocPageOutput
from idp.runners.normalization_helpers import standardize_text
from idp.runners.record_worker import RecordWorker


class DocPagesWorker(RecordWorker):
    is_spark_serializable = False
    is_threadsafe = True

    def _page_from_normalized(self, norm_json: str, fallback_page_number: Any) -> tuple:
        try:
            d = json.loads(norm_json) if norm_json else {}
            pages = (d.get("text", {}) or {}).get("pages") or []

            page = None
            target_page = None
            if fallback_page_number is not None:
                try:
                    target_page = int(fallback_page_number)
                except (TypeError, ValueError):
                    target_page = None

            if pages:
                if target_page is not None:
                    page = next(
                        (p for p in pages if p.get("page_number") == target_page),
                        None,
                    )
                if page is None:
                    page = pages[0]

            if page:
                pnum = int(page.get("page_number", target_page or 1))
                lines = page.get("lines") or []
                if lines:
                    page_text = "\n".join(
                        [
                            ln.get("content", "")
                            for ln in lines
                            if (ln.get("content") or "").strip()
                        ]
                    )
                else:
                    page_text = (d.get("text", {}) or {}).get("full") or ""

                confs = []
                for w in (page.get("words") or []):
                    c = w.get("confidence")
                    if isinstance(c, (int, float)):
                        confs.append(float(c))

                if confs:
                    avg_conf = sum(confs) / len(confs)
                    low_conf = sum(1 for c in confs if c < 0.6)
                    total = len(confs)
                else:
                    avg_conf, low_conf, total = (None, 0, 0)

                return (pnum, page_text, avg_conf, low_conf, total)

            full = (d.get("text", {}) or {}).get("full") or ""
            return (target_page or 1, full, None, 0, 0)
        except Exception:
            return (1, "", None, 0, 0)

    def process(self, record) -> List[DocPageOutput]:
        norm_json = getattr(record, "normalized_response_json", "") or ""
        parser_type = getattr(record, "parser_type", "")
        docuid = getattr(record, "docuid", "")
        page_number = getattr(record, "page_number", None)

        source = ""
        doc_kind = ""
        try:
            payload = json.loads(norm_json) if norm_json else {}
            source = payload.get("source", "")
            doc_kind = payload.get("doc_kind", "")
        except Exception:
            source = ""
            doc_kind = ""

        pnum, page_text, avg_conf, low_conf, total = self._page_from_normalized(
            norm_json, page_number
        )
        return [
            DocPageOutput(
                docuid=docuid,
                parser_type=parser_type,
                source=source,
                doc_kind=doc_kind,
                normalized_response_json=norm_json,
                page_number=pnum,
                extracted_page_text=page_text,
                page_avg_conf=avg_conf,
                page_low_conf_words=low_conf,
                page_total_words=total,
                standardized_page_text=standardize_text(page_text),
            )
        ]
