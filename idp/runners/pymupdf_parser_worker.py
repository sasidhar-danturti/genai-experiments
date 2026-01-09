from __future__ import annotations

import fitz

from idp.db_manager.spark_models import PyMuPDFResponseOutput, RoutePredictionOutput
from idp.runners.record_worker import RecordWorker


class PyMuPDFParserWorker(RecordWorker):
    is_spark_serializable = False
    is_threadsafe = True

    def process(self, record: RoutePredictionOutput) -> PyMuPDFResponseOutput:
        response = None
        status = "ERRORED"
        if record.downloaded_attachment_path and record.page_number:
            doc = fitz.open(record.downloaded_attachment_path)
            page = doc[record.page_number - 1]
            response = page.get_text()
            doc.close()
            status = "COMPLETED"

        return PyMuPDFResponseOutput(
            docuid=record.docuid,
            final_docuid=record.final_docuid,
            downloaded_attachment_path=record.downloaded_attachment_path,
            page_number=record.page_number,
            parser_response=response,
            parser_type="PyMuPDF",
            status=status,
        )
