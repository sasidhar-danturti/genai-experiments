from abc import ABC, abstractmethod
from typing import Iterable, List, Optional, Type, Any
from idp.db_manager.spark_models import SparkModel


class DBAdapter(ABC):
    @abstractmethod
    def qualified_table(self, tier: str, table: str) -> str:
        ...

    @abstractmethod
    def write(self, tier: str, data: SparkModel) -> None:
        ...

    @abstractmethod
    def write_batch(self, tier: str, data_list: Iterable[SparkModel]) -> None:
        ...

    @abstractmethod
    def read(self, tier: str, model: Type[SparkModel], filter_condition: Optional[str] = None) -> List[SparkModel]:
        ...

    @abstractmethod
    def read_dataframe(self, tier: str, table: str, filter_condition: Optional[str] = None):
        ...

    @abstractmethod
    def update_records(self, tier: str, model_instance: SparkModel, fields_to_update: List[str]) -> None:
        ...

    def write_dlq(self, tier: str, data: SparkModel) -> None:
        ...

    def checkpoint(self, key: str, payload: Any) -> None:
        ...

    def read_checkpoint(self, key: str) -> Any:
        ...


def make_adapter(config) -> DBAdapter:
    backend = config.database.backend.lower()
    if backend == "uc":
        from idp.db_manager.spark_uc_adapter import SparkUCAdapter
        return SparkUCAdapter(config)
    if backend == "lakehouse":
        from idp.db_manager.lakehouse_adapter import LakehouseAdapter
        return LakehouseAdapter(config)
    raise ValueError(f"Unsupported database backend: {backend}")
