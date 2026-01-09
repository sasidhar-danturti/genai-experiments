from __future__ import annotations

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from idp.db_manager.spark_models import NormalizedResponseOutput
from idp.runners.base_runner import BaseRunner
from idp.runners.normalization_worker import NormalizedResponseWorker


class NormalizedResponseRunner(BaseRunner):
    input_model = NormalizedResponseOutput
    output_model = NormalizedResponseOutput
    input_table = "current_pymupdf_responses"
    history_table = "current_normalised_responses"

    source_tables = {
        "adi_llm": "current_adi_llm_responses",
        "adi": "current_adi_responses",
        "llm": "current_llm_responses",
        "pymupdf": "current_pymupdf_responses",
    }

    def __init__(self, adapter=None, planner=None, is_first: bool = True, is_last: bool = True) -> None:
        super().__init__(
            worker=NormalizedResponseWorker(),
            adapter=adapter,
            planner=planner,
            is_first=is_first,
            is_last=is_last,
        )

    def _load_sources(self) -> DataFrame:
        dfs = []
        for table in self.source_tables.values():
            df = self.adapter.read_dataframe("bronze", table)
            select_df = df.select(
                "docuid",
                "final_docuid",
                "parser_type",
                "parser_response",
                "page_number",
            )
            dfs.append(select_df)
        return reduce(lambda left, right: left.unionByName(right), dfs)

    def run(self, df_or_records: DataFrame = None, ctx_overrides: dict | None = None):
        if df_or_records is None:
            df_or_records = self._load_sources()
        overrides = {"preferred_strategy": "spark_partition"}
        if ctx_overrides:
            overrides.update(ctx_overrides)
        return super().run(df_or_records, ctx_overrides=overrides)
