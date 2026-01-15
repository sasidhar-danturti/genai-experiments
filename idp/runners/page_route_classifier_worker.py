from __future__ import annotations

import json
import os
from typing import Optional

from idp.db_manager.spark_models import PageImageExtractionOutput, RoutePredictionOutput
from idp.runners.record_worker import RecordWorker


class PageRouteClassifierWorker(RecordWorker):
    is_spark_serializable = False
    is_threadsafe = True

    def __init__(
        self,
        api_key: Optional[str] = None,
        azure_endpoint: Optional[str] = None,
        model: Optional[str] = None,
    ) -> None:
        self._api_key = api_key or os.getenv("IDP_OPENAI_API_KEY")
        self._azure_endpoint = azure_endpoint or os.getenv("IDP_OPENAI_ENDPOINT")
        self._model = model or os.getenv("IDP_OPENAI_MODEL", "gpt-4o")
        self._classifier = None

    def _get_classifier(self):
        if self._classifier is None:
            if not self._api_key or not self._azure_endpoint:
                raise ValueError("Classifier configuration missing (API key/endpoint).")
            from idp.router import Classifier

            self._classifier = Classifier(
                api_key=self._api_key,
                azure_endpoint=self._azure_endpoint,
                model=self._model,
            )
        return self._classifier

    def process(self, record: PageImageExtractionOutput) -> RoutePredictionOutput:
        prediction_json = None
        route_decision = None
        status = "ERRORED"

        if record.page_image:
            classifier = self._get_classifier()
            prediction = json.loads(classifier.predict(record.page_image, "Classify the Image"))
            if record.file_extension == "pdf" and prediction.get("route") == "NO_PARSE":
                prediction["route"] = "ADI"
            prediction_json = json.dumps(prediction)
            route_decision = prediction.get("route")
            status = "COMPLETED"

        return RoutePredictionOutput(
            docuid=record.docuid,
            final_docuid=record.final_docuid,
            attachment_name=record.attachment_name,
            downloaded_attachment_path=record.downloaded_attachment_path,
            file_extension=record.file_extension,
            file_name=record.file_name,
            page_image=record.page_image,
            page_number=record.page_number,
            route_prediction_json=prediction_json,
            route_decision=route_decision,
            status=status,
        )
