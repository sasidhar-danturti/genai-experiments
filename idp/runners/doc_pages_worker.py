from __future__ import annotations

import json
from typing import Any, List

from idp.db_manager.spark_models import DocPageOutput
from idp.runners.normalization_helpers import standardize_text
from idp.runners.record_worker import RecordWorker


class DocPagesWorker(RecordWorker):
    is_spark_serializable = False
    is_threadsafe = True

    def _pages_from_normalized(self, norm_json: str, fallback_page_number: Any) -> List[tuple]:
        try:
            d = json.loads(norm_json) if norm_json else {}
            pages = (d.get("text", {}) or {}).get("pages") or []

            out = []
            for p in pages:
                pnum = int(p.get("page_number", fallback_page_number or 1))

                lines = p.get("lines") or []
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
                for w in (p.get("words") or []):
                    c = w.get("confidence")
                    if isinstance(c, (int, float)):
                        confs.append(float(c))

                if confs:
                    avg_conf = sum(confs) / len(confs)
                    low_conf = sum(1 for c in confs if c < 0.6)
                    total = len(confs)
                else:
                    avg_conf, low_conf, total = (None, 0, 0)

                out.append((pnum, page_text, avg_conf, low_conf, total))

            if not out:
                full = (d.get("text", {}) or {}).get("full") or ""
                out = [(1, full, None, 0, 0)]

            return out
        except Exception:
            return [(1, "", None, 0, 0)]

    def process(self, record) -> List[DocPageOutput]:
        outputs: List[DocPageOutput] = []
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

        for pnum, page_text, avg_conf, low_conf, total in self._pages_from_normalized(
            norm_json, page_number
        ):
            outputs.append(
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
            )

        return outputs
