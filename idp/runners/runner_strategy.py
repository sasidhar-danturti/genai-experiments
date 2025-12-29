from abc import ABC, abstractmethod
from typing import Any, Iterable, List
from loguru import logger
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import DataFrame

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
                outputs.append(fut.result())
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
        try:
            row_count = df.count()
            if ctx.partition_size and row_count > ctx.partition_size:
                target_parts = max(1, row_count // ctx.partition_size)
                df = df.repartition(target_parts)
        except Exception:
            pass

        def process_partition(rows_iter):
            local_worker = worker if worker.is_spark_serializable else type(worker)()
            for row in rows_iter:
                attempt = 1
                while True:
                    try:
                        yield local_worker.process(row)
                        break
                    except Exception:
                        if attempt >= ctx.max_attempts:
                            raise
                        time.sleep(ctx.backoff_seconds * attempt)
                        attempt += 1

        rdd = df.rdd.mapPartitions(process_partition)
        return rdd.collect()
