import uuid
from typing import Dict, Optional, Sequence

from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from idp.config_manager import CONFIG
from idp.db_manager.db_adapter import DBAdapter, make_adapter
from idp.db_manager.spark_models import (
    Batch,
    BatchState,
    FileProcess,
    TaskExecution,
    TaskExecutionState,
)
from idp.runtime_context import DEPENDENCY_PROVIDER


class AggregationRunner:
    required_class_vars = ("input_table", "output_tables")
    input_table: Optional[str] = None
    output_tables: Optional[Sequence[str]] = None
    output_tier: str = "bronze"

    def __init__(
        self,
        adapter: Optional[DBAdapter] = None,
        is_first: bool = True,
        is_last: bool = True,
    ) -> None:
        self.adapter = adapter or make_adapter(CONFIG)
        self.is_first = is_first
        self.is_last = is_last
        self.spark = SparkSession.getActiveSession()
        self.batch_id = self._get_or_generate("batch")
        self.task_id = self._get_or_generate("run")
        self.runner_name = self.__class__.__name__

        Batch.set_table_name(CONFIG.audit_tables.batch)
        FileProcess.set_table_name(CONFIG.audit_tables.file_process)
        TaskExecution.set_table_name(CONFIG.audit_tables.task_execution)
        self._record_start(CONFIG, self.runner_name)

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        missing = [
            name
            for name in cls.required_class_vars
            if getattr(cls, name, None) is None
        ]
        if missing:
            raise TypeError(
                f"{cls.__name__} must define class variables: {', '.join(missing)}"
            )

    def _get_or_generate(self, kind: str) -> str:
        from idp.utils.databricks_utils import get_databricks_utils

        utils = get_databricks_utils()
        getter = {
            "batch": utils.get_current_batch_id,
            "run": utils.get_current_run_id,
        }.get(kind)
        try:
            return getter()
        except Exception:
            return f"local_{kind}_{uuid.uuid4()}"

    def _record_start(self, config, runner_name: str) -> None:
        config_json = config.json()
        if self.is_first:
            batch = Batch(
                config_json=config_json,
                runner_name=runner_name,
                batch_id=self.batch_id,
                status=BatchState.START,
            )
            self.batch_obj = batch
            self.adapter.write("bronze", batch)
        else:
            batches = self.adapter.read(
                "bronze",
                Batch,
                filter_condition=f"batch_id = '{self.batch_id}'",
            )
            self.batch_obj = max(batches, key=lambda obj: obj.created_at)

        task = TaskExecution(
            batch_id=self.batch_id,
            task_name=runner_name,
            status=TaskExecutionState.START,
            task_id=self.task_id,
        )
        self.task_obj = task
        self.adapter.write("bronze", task)
        DEPENDENCY_PROVIDER.update_context(
            run_id=self.task_id,
            batch_id=self.batch_id,
            file_id="NA",
        )

    def _update_audit(self, errored: bool) -> None:
        task_status = (
            TaskExecutionState.FAILED.value
            if errored
            else TaskExecutionState.COMPLETED.value
        )
        self.task_obj.status = task_status
        self.adapter.update_records("bronze", self.task_obj, ["status"])
        if self.is_last:
            batch_status = (
                BatchState.COMPLETED_WITH_ERRORS.value
                if errored
                else BatchState.COMPLETED.value
            )
            self.batch_obj.status = batch_status
            self.adapter.update_records("bronze", self.batch_obj, ["status"])

    def load_inputs_dataframe(self, tier: str = "bronze", filter_condition: Optional[str] = None) -> DataFrame:
        if not self.input_table:
            raise ValueError("input_table must be set to load a DataFrame.")
        return self.adapter.read_dataframe(tier, self.input_table, filter_condition=filter_condition)

    def build_outputs(self, df: DataFrame) -> Dict[str, DataFrame]:
        raise NotImplementedError

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
