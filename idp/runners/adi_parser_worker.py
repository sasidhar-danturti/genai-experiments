from __future__ import annotations

import os

from idp.db_manager.spark_models import ADIResponseOutput, RoutePredictionOutput
from idp.doc_services.azure_adi.mock_adi import MockDocumentProcessor
from idp.runners.record_worker import RecordWorker


class ADIParserWorker(RecordWorker):
    is_spark_serializable = False
    is_threadsafe = True

    def __init__(self, model: str | None = None, document_path: str | None = None) -> None:
        self._model = model or os.getenv("IDP_ADI_MODEL", "prebuilt_layout")
        self._document_path = document_path or os.getenv("IDP_ADI_DOCUMENT_PATH", "/test")
        self._processor = MockDocumentProcessor(model=self._model)

    def process(self, record: RoutePredictionOutput) -> ADIResponseOutput:
        response = None
        status = "ERRORED"

        if record.file_name and record.page_number:
            response = self._processor.process_document(
                document_name=record.file_name,
                page_number=record.page_number,
                document_path=self._document_path,
            )
            status = "COMPLETED"

        return ADIResponseOutput(
            docuid=record.docuid,
            final_docuid=record.final_docuid,
            file_name=record.file_name,
            page_number=record.page_number,
            parser_response=str(response) if response is not None else None,
            parser_type="ADI",
            status=status,
        )
