from __future__ import annotations

from pyspark.sql import DataFrame

from idp.db_manager.spark_models import ADILLMResponseOutput, RoutePredictionOutput
from idp.runners.adi_llm_parser_worker import ADILLMParserWorker
from idp.runners.base_runner import BaseRunner


class ADILLMParserRunner(BaseRunner):
    input_model = RoutePredictionOutput
    output_model = ADILLMResponseOutput
    input_table = "current_routes_responses_ADI_LLM"
    history_table = "current_adi_llm_responses"

    def __init__(self, adapter=None, planner=None, is_first: bool = True, is_last: bool = True) -> None:
        super().__init__(
            worker=ADILLMParserWorker(),
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
