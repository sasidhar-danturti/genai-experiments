from typing import Any
from pyspark.sql import DataFrame

from idp.runners.runner_strategy import RunnerStrategy, DriverThreadPoolStrategy, SparkPartitionStrategy
from idp.runners.runner_context import RunnerContext
from idp.runners.record_worker import RecordWorker


class RunnerPlanner:
    def pick(self, records_or_df: Any, worker: RecordWorker, ctx: RunnerContext) -> RunnerStrategy:
        if ctx.preferred_strategy == "driver_thread":
            return DriverThreadPoolStrategy()
        if ctx.preferred_strategy == "spark_partition":
            return SparkPartitionStrategy()

        if isinstance(records_or_df, DataFrame):
            if not worker.is_spark_serializable:
                return DriverThreadPoolStrategy()
            return SparkPartitionStrategy()

        return DriverThreadPoolStrategy()
