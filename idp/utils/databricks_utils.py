from __future__ import annotations

import json
from typing import Optional

from loguru import logger
from pyspark.sql import SparkSession


class DatabricksUtils:
    def __init__(self) -> None:
        self._dbutils = None
        self._init_dbutils()

    def _init_dbutils(self) -> None:
        if self._dbutils is not None:
            return
        try:
            from pyspark.dbutils import DBUtils

            spark = SparkSession.getActiveSession()
            if spark is None:
                logger.debug("No active SparkSession; DBUtils not initialized.")
                return
            self._dbutils = DBUtils(spark)
        except ImportError:
            logger.debug("pyspark.dbutils not available; running outside Databricks.")
        except Exception as exc:
            logger.warning(f"Failed to initialize DBUtils: {exc}")

    def _get_context(self) -> dict:
        if self._dbutils is None:
            return {"tags": {}}
        try:
            context_str = (
                self._dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .toJson()
            )
            return json.loads(context_str)
        except Exception as exc:
            raise RuntimeError(f"Error retrieving notebook context: {exc}") from exc

    def get_current_batch_id(self) -> str:
        context = self._get_context()
        tags = context.get("tags", {})
        session_id = tags.get("sessionId", "unknown")
        return tags.get("jobRunId", f"local_batch_{session_id}")

    def get_current_run_id(self) -> str:
        context = self._get_context()
        tags = context.get("tags", {})
        return tags.get("runId", f"local_task_{context.get('commandId', 'unknown')}")

    def get_secret(self, scope_name: str, secret_key: str) -> str:
        self._init_dbutils()
        if self._dbutils is None:
            raise RuntimeError("DBUtils not available in this environment.")
        try:
            return self._dbutils.secrets.get(scope=scope_name, key=secret_key)
        except Exception as exc:
            raise RuntimeError(
                f"Error accessing secret '{secret_key}' in scope '{scope_name}': {exc}"
            ) from exc


_data_bricks_utils: Optional[DatabricksUtils] = None


def get_databricks_utils() -> DatabricksUtils:
    global _data_bricks_utils
    if _data_bricks_utils is None:
        _data_bricks_utils = DatabricksUtils()
    return _data_bricks_utils
