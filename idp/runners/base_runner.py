import uuid
from typing import Any, List, Optional
from loguru import logger
from pyspark.sql import SparkSession, DataFrame

from idp.runners.runner_context import RunnerContext
from idp.runners.runner_planner import RunnerPlanner
from idp.runners.runner_strategy import RunnerStrategy
from idp.runners.record_worker import RecordWorker

from idp.runtime_context import DEPENDENCY_PROVIDER
from idp.config_manager import CONFIG
from idp.db_manager.db_adapter import DBAdapter, make_adapter
from idp.db_manager.spark_models import (
    Batch,
    FileProcess,
    FileProcessState,
    BatchState,
    TaskExecution,
    TaskExecutionState,
)


class BaseRunner:
    required_class_vars = ("input_model", "output_model", "input_table")
    input_model = None
    output_model = None
    input_table: Optional[str] = None
    history_table: Optional[str] = None
    idempotency_keys: List[str] = []
    dlq_table: Optional[str] = None

    def __init__(
        self,
        worker: RecordWorker,
        adapter: Optional[DBAdapter] = None,
        planner: Optional[RunnerPlanner] = None,
        is_first: bool = True,
        is_last: bool = True,
    ) -> None:
        self.worker = worker
        self.adapter = adapter or make_adapter(CONFIG)
        self.planner = planner or RunnerPlanner()
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

    def _dedupe(self, records: List[Any]) -> List[Any]:
        if not self.idempotency_keys:
            return records
        seen = set()
        deduped = []
        for record in records:
            key = tuple(getattr(record, k, None) for k in self.idempotency_keys)
            if key not in seen:
                seen.add(key)
                deduped.append(record)
        return deduped

    def _make_file_process(
        self,
        record: Any,
        status: FileProcessState,
        error_message: str = "",
    ) -> FileProcess:
        file_name = getattr(record, "docuid", getattr(record, "id", "unknown"))
        file_process = FileProcess(
            file_name=file_name,
            batch_id=self.batch_id,
            task_id=self.task_id,
            status=status,
            error_message=error_message,
        )
        DEPENDENCY_PROVIDER.update_context(
            run_id=self.task_id,
            batch_id=self.batch_id,
            file_id=file_process.id,
        )
        return file_process

    def _collect_and_dedupe_df(self, df: DataFrame, ctx: RunnerContext) -> Any:
        if ctx.preferred_strategy == "driver_thread":
            return [self.input_model.from_spark_row(r) for r in df.collect()]
        if ctx.max_driver_records is not None:
            sample = df.limit(ctx.max_driver_records + 1).collect()
            if len(sample) <= ctx.max_driver_records:
                return [self.input_model.from_spark_row(r) for r in sample]
        return df

    def _attach_context(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            obj["batch_id"] = self.batch_id
            obj["task_id"] = self.task_id
            return obj

        if hasattr(obj, "model_copy"):
            try:
                fields = getattr(obj, "model_fields", None) or getattr(obj, "__fields__", {})
                if "batch_id" in fields or "task_id" in fields:
                    return obj.model_copy(
                        update={"batch_id": self.batch_id, "task_id": self.task_id}
                    )
            except Exception:
                pass

        if hasattr(obj, "copy"):
            try:
                fields = getattr(obj, "__fields__", {})
                if "batch_id" in fields or "task_id" in fields:
                    return obj.copy(
                        update={"batch_id": self.batch_id, "task_id": self.task_id}
                    )
            except Exception:
                pass

        for key, value in (("batch_id", self.batch_id), ("task_id", self.task_id)):
            try:
                setattr(obj, key, value)
            except AttributeError:
                if hasattr(obj, "__dict__"):
                    obj.__dict__[key] = value
        return obj

    def _is_failure_output(self, out: Any) -> bool:
        status = getattr(out, "status", "").lower()
        return status in {"errored", "failed", "error"}

    def load_inputs_dataframe(self, tier: str = "bronze", filter_condition: Optional[str] = None):
        if not self.input_table:
            raise ValueError("input_table must be set to load a DataFrame.")
        return self.adapter.read_dataframe(tier, self.input_table, filter_condition=filter_condition)

    def run(self, df_or_records: Any = None, ctx_overrides: Optional[dict] = None) -> List[Any]:
        ctx = RunnerContext(
            batch_id=self.batch_id,
            task_id=self.task_id,
            runner_name=self.runner_name,
            worker_caps={
                "is_spark_serializable": getattr(self.worker, "is_spark_serializable", True),
                "is_threadsafe": getattr(self.worker, "is_threadsafe", True),
            },
        )
        if ctx_overrides:
            for key, value in ctx_overrides.items():
                setattr(ctx, key, value)

        outputs: List[Any] = []
        status_buffer: List[FileProcess] = []
        dlq_buffer: List[FileProcess] = []

        try:
            input_obj = df_or_records
            if input_obj is None:
                if ctx.preferred_strategy == "spark_partition":
                    input_obj = self.load_inputs_dataframe()
                else:
                    raise ValueError("df_or_records must be provided when not using spark_partition.")
            if isinstance(input_obj, DataFrame):
                input_obj = self._collect_and_dedupe_df(input_obj, ctx)

            if not isinstance(input_obj, DataFrame):
                if (
                    isinstance(input_obj, list)
                    and not self.idempotency_keys
                    and all(isinstance(r, self.input_model) for r in input_obj)
                ):
                    records = input_obj
                else:
                    records = [
                        self.input_model.from_spark_row(r)
                        if not isinstance(r, self.input_model)
                        else r
                        for r in input_obj
                    ]
                    records = self._dedupe(records)
                input_obj = records

            strategy: RunnerStrategy = self.planner.pick(input_obj, self.worker, ctx)
            outputs = strategy.execute(input_obj, self.worker, ctx)

            outputs = [self._attach_context(output) for output in outputs]

            failed_outputs = [o for o in outputs if self._is_failure_output(o)]
            success_outputs = [o for o in outputs if o not in failed_outputs]

            for output in success_outputs:
                status_buffer.append(
                    self._make_file_process(
                        output,
                        FileProcessState.DOWNLOAD_COMPLETED.value,
                    )
                )
            for output in failed_outputs:
                error_message = getattr(output, "error_message", "")
                status_buffer.append(
                    self._make_file_process(
                        output,
                        FileProcessState.DOWNLOAD_FAILED.value,
                        error_message,
                    )
                )
                if self.dlq_table:
                    dlq_buffer.append(
                        self._make_file_process(
                            output,
                            FileProcessState.DOWNLOAD_FAILED.value,
                            error_message,
                        )
                    )

            if outputs:
                self.adapter.write_batch(ctx.output_tier, outputs)
                if self.history_table:
                    original_table = getattr(self.output_model, "__tablename__", None)
                    self.output_model.set_table_name(self.history_table)
                    try:
                        self.adapter.write_batch(ctx.output_tier, outputs)
                    finally:
                        if original_table:
                            self.output_model.set_table_name(original_table)

            if status_buffer:
                self.adapter.write_batch("bronze", status_buffer)

            if dlq_buffer:
                for file_process in dlq_buffer:
                    self.adapter.write_dlq("bronze", file_process)

            self._update_audit(errored=bool(failed_outputs))
            return outputs

        except Exception as error:
            logger.exception(f"Runner failed: {error}")
            try:
                if status_buffer:
                    self.adapter.write_batch("bronze", status_buffer)
                if dlq_buffer:
                    for file_process in dlq_buffer:
                        self.adapter.write_dlq("bronze", file_process)
            except Exception:
                logger.exception("Failed to flush buffers during failure handling")
            self._update_audit(errored=True)
            raise
