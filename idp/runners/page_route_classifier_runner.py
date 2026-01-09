from __future__ import annotations

from pyspark.sql import DataFrame

from idp.db_manager.spark_models import PageImageExtractionOutput, RoutePredictionOutput
from idp.runners.base_runner import BaseRunner
from idp.runners.page_route_classifier_worker import PageRouteClassifierWorker


class PageRouteClassifierRunner(BaseRunner):
    input_model = PageImageExtractionOutput
    output_model = RoutePredictionOutput
    input_table = "current_docs_with_page_images"
    history_table = "current_routes_responses"

    def __init__(
        self,
        adapter=None,
        planner=None,
        is_first: bool = True,
        is_last: bool = True,
        api_key: str | None = None,
        azure_endpoint: str | None = None,
        model: str | None = None,
    ) -> None:
        super().__init__(
            worker=PageRouteClassifierWorker(
                api_key=api_key,
                azure_endpoint=azure_endpoint,
                model=model,
            ),
            adapter=adapter,
            planner=planner,
            is_first=is_first,
            is_last=is_last,
        )

    def run(self, df_or_records: DataFrame = None, ctx_overrides: dict | None = None):
        if df_or_records is None:
            df_or_records = self.load_inputs_dataframe()
        overrides = {"preferred_strategy": "spark_partition"}
        if ctx_overrides:
            overrides.update(ctx_overrides)
        return super().run(df_or_records, ctx_overrides=overrides)
