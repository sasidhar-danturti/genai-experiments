from abc import ABC, abstractmethod
from typing import Dict, Optional, Sequence

from pyspark.sql import DataFrame

from idp.runners.base_runner import BaseRunner
from idp.runners.record_worker import RecordWorker


class _AggregationWorker(RecordWorker):
    is_spark_serializable: bool = False
    is_threadsafe: bool = True

    def process(self, record):
        raise NotImplementedError("AggregationRunner does not process records.")


class AggregationRunner(BaseRunner, ABC):
    input_table: Optional[str] = None
    output_tables: Optional[Sequence[str]] = None
    output_tier: str = "bronze"

    def __init_subclass__(cls, **kwargs) -> None:
        super(BaseRunner, cls).__init_subclass__(**kwargs)
        if getattr(cls, "__abstractmethods__", None):
            return
        missing = [
            name
            for name in ("input_table", "output_tables")
            if getattr(cls, name, None) is None
        ]
        if missing:
            raise TypeError(
                f"{cls.__name__} must define class variables: {', '.join(missing)}"
            )

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
        return super().run_dataframe(
            df_or_records,
            self.build_outputs,
            output_tables=self.output_tables,
            output_tier=self.output_tier,
        )
