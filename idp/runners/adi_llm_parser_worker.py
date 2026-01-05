from __future__ import annotations

import json
import os

from idp.db_manager.spark_models import ADILLMResponseOutput, RoutePredictionOutput
from idp.doc_services.azure_adi.mock_adi import MockDocumentProcessor
from idp.doc_parser.llm_text_parser import LLMTextParser
from idp.runners.record_worker import RecordWorker


class ADILLMParserWorker(RecordWorker):
    is_spark_serializable = False
    is_threadsafe = True

    def __init__(
        self,
        adi_model: str | None = None,
        document_path: str | None = None,
        api_key: str | None = None,
        azure_endpoint: str | None = None,
        model: str | None = None,
    ) -> None:
        self._adi_model = adi_model or os.getenv("IDP_ADI_MODEL", "prebuilt_layout")
        self._document_path = document_path or os.getenv("IDP_ADI_DOCUMENT_PATH", "/test")
        self._processor = MockDocumentProcessor(model=self._adi_model)
        self._api_key = api_key or os.getenv("IDP_OPENAI_API_KEY")
        self._azure_endpoint = azure_endpoint or os.getenv("IDP_OPENAI_ENDPOINT")
        self._model = model or os.getenv("IDP_OPENAI_MODEL", "gpt-4o")
        self._llm_parser = None

    def _get_llm_parser(self) -> LLMTextParser:
        if self._llm_parser is None:
            if not self._api_key or not self._azure_endpoint:
                raise ValueError("LLM parser configuration missing (API key/endpoint).")
            self._llm_parser = LLMTextParser(
                api_key=self._api_key,
                azure_endpoint=self._azure_endpoint,
                model=self._model,
            )
        return self._llm_parser

    def process(self, record: RoutePredictionOutput) -> ADILLMResponseOutput:
        status = "ERRORED"
        parser_response = None

        if record.file_name and record.page_number:
            adi_result = self._processor.process_document(
                document_name=record.file_name,
                page_number=record.page_number,
                document_path=self._document_path,
            )
            llm_parser = self._get_llm_parser()
            llm_result = llm_parser.predict(record.page_image, str(adi_result))
            parser_response = json.dumps(
                {"adi_result": str(adi_result), "llm_result": llm_result}
            )
            status = "COMPLETED"

        return ADILLMResponseOutput(
            docuid=record.docuid,
            final_docuid=record.final_docuid,
            page_image=record.page_image,
            parser_response=parser_response,
            parser_type="ADI_LLM",
            status=status,
        )
