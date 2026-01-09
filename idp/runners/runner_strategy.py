from abc import ABC, abstractmethod
from typing import Any, Iterable, List
from loguru import logger
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import DataFrame

from idp.runtime_context import get_runtime_context, set_runtime_context
from idp.runners.runner_context import RunnerContext
from idp.runners.record_worker import RecordWorker


class RunnerStrategy(ABC):
    @abstractmethod
    def execute(self, records_or_df: Any, worker: RecordWorker, ctx: RunnerContext) -> List[Any]:
        ...


class DriverThreadPoolStrategy(RunnerStrategy):
    def execute(self, records_or_df: Iterable[Any], worker: RecordWorker, ctx: RunnerContext) -> List[Any]:
        records = list(records_or_df)
        outputs: List[Any] = []
        with ThreadPoolExecutor(max_workers=ctx.max_concurrency) as pool:
            fut_map = {pool.submit(self._run_with_retry, worker, rec, ctx): rec for rec in records}
            for fut in as_completed(fut_map):
                result = fut.result()
                if isinstance(result, list):
                    outputs.extend(result)
                else:
                    outputs.append(result)
        return outputs

    def _run_with_retry(self, worker, rec, ctx):
        attempt = 1
        while True:
            try:
                return worker.process(rec)
            except Exception as e:
                if attempt >= ctx.max_attempts:
                    logger.exception(f"Record failed after {attempt} attempts: {e}")
                    raise
                time.sleep(ctx.backoff_seconds * attempt)
                attempt += 1


class SparkPartitionStrategy(RunnerStrategy):
    def execute(self, records_or_df: DataFrame, worker: RecordWorker, ctx: RunnerContext) -> List[Any]:
        df = records_or_df
        spark = df.sparkSession
        context_snapshot = get_runtime_context()
        context_broadcast = spark.sparkContext.broadcast(context_snapshot)
        if ctx.partition_size:
            try:
                current_parts = df.rdd.getNumPartitions()
                target_parts = max(1, current_parts)
                df = df.repartition(target_parts)
            except Exception:
                pass

        def process_partition(rows_iter):
            set_runtime_context(context_broadcast.value)
            local_worker = worker if worker.is_spark_serializable else type(worker)()
            for row in rows_iter:
                attempt = 1
                while True:
                    try:
                        result = local_worker.process(row)
                        if isinstance(result, list):
                            for item in result:
                                yield item
                        else:
                            yield result
                        break
                    except Exception:
                        if attempt >= ctx.max_attempts:
                            raise
                        time.sleep(ctx.backoff_seconds * attempt)
                        attempt += 1

        rdd = df.rdd.mapPartitions(process_partition)
        return rdd.collect()
