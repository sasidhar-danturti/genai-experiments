from abc import ABC, abstractmethod
from typing import Dict, Optional, Sequence

from loguru import logger
from pyspark.sql import DataFrame

from idp.runners.base_runner import BaseRunner
from idp.runners.record_worker import RecordWorker


class _AggregationWorker(RecordWorker):
    is_spark_serializable: bool = False
    is_threadsafe: bool = True

    def process(self, record):
        raise NotImplementedError("AggregationRunner does not process records.")


class AggregationRunner(BaseRunner, ABC):
    required_class_vars = ("input_table", "output_tables")
    input_table: Optional[str] = None
    output_tables: Optional[Sequence[str]] = None
    output_tier: str = "bronze"

    def __init__(
        self,
        adapter=None,
        planner=None,
        is_first: bool = True,
        is_last: bool = True,
    ) -> None:
        super().__init__(
            worker=_AggregationWorker(),
            adapter=adapter,
            planner=planner,
            is_first=is_first,
            is_last=is_last,
        )

    @abstractmethod
    def build_outputs(self, df: DataFrame) -> Dict[str, DataFrame]:
        ...

    def run(self, df_or_records: Optional[DataFrame] = None) -> Dict[str, DataFrame]:
        try:
            if df_or_records is None:
                df_or_records = self.load_inputs_dataframe()
            outputs = self.build_outputs(df_or_records)
            if self.output_tables:
                missing = set(self.output_tables) - set(outputs.keys())
                if missing:
                    raise ValueError(
                        f"Missing outputs for tables: {', '.join(sorted(missing))}"
                    )
            for table_name, output_df in outputs.items():
                self.adapter.write_dataframe(self.output_tier, table_name, output_df)
            self._update_audit(errored=False)
            return outputs
        except Exception as error:
            logger.exception(f"Aggregation runner failed: {error}")
            self._update_audit(errored=True)
            raise
