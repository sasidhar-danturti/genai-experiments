from __future__ import annotations

import os

from idp.db_manager.spark_models import LLMResponseOutput, RoutePredictionOutput
from idp.doc_parser.llm_image_parser import LLMImageParser
from idp.runners.record_worker import RecordWorker


class LLMParserWorker(RecordWorker):
    is_spark_serializable = False
    is_threadsafe = True

    def __init__(
        self,
        api_key: str | None = None,
        azure_endpoint: str | None = None,
        model: str | None = None,
    ) -> None:
        self._api_key = api_key or os.getenv("IDP_OPENAI_API_KEY")
        self._azure_endpoint = azure_endpoint or os.getenv("IDP_OPENAI_ENDPOINT")
        self._model = model or os.getenv("IDP_OPENAI_MODEL", "gpt-4o")
        self._parser = None

    def _get_parser(self) -> LLMImageParser:
        if self._parser is None:
            if not self._api_key or not self._azure_endpoint:
                raise ValueError("LLM parser configuration missing (API key/endpoint).")
            self._parser = LLMImageParser(
                api_key=self._api_key,
                azure_endpoint=self._azure_endpoint,
                model=self._model,
            )
        return self._parser

    def process(self, record: RoutePredictionOutput) -> LLMResponseOutput:
        response = None
        status = "ERRORED"

        if record.page_image:
            parser = self._get_parser()
            response = parser.predict(record.page_image, "Analyze")
            status = "COMPLETED"

        return LLMResponseOutput(
            docuid=record.docuid,
            final_docuid=record.final_docuid,
            page_image=record.page_image,
            parser_response=response,
            parser_type="LLM",
            status=status,
        )
