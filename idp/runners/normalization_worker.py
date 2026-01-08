from __future__ import annotations

from idp.db_manager.spark_models import NormalizedResponseOutput
from idp.runners.normalization_helpers import normalize_by_type
from idp.runners.record_worker import RecordWorker


class NormalizedResponseWorker(RecordWorker):
    is_spark_serializable = False
    is_threadsafe = True

    def process(self, record) -> NormalizedResponseOutput:
        normalized_json = normalize_by_type(
            getattr(record, "parser_type", None),
            getattr(record, "parser_response", None),
        )
        return NormalizedResponseOutput(
            docuid=getattr(record, "docuid", ""),
            final_docuid=getattr(record, "final_docuid", None),
            parser_type=getattr(record, "parser_type", "unknown"),
            parser_response=getattr(record, "parser_response", None),
            page_number=getattr(record, "page_number", None),
            normalized_response_json=normalized_json,
            status="COMPLETED",
        )
