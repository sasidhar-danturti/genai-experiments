from typing import Iterable, List, Optional, Type
from idp.db_manager.db_adapter import DBAdapter
from idp.db_manager.spark_models import SparkModel


class LakehouseAdapter(DBAdapter):
    def __init__(self, config):
        self.config = config

    def qualified_table(self, tier: str, table: str) -> str:
        raise NotImplementedError("LakehouseAdapter.qualified_table is a stub.")

    def write(self, tier: str, data: SparkModel) -> None:
        raise NotImplementedError("LakehouseAdapter.write is a stub.")

    def write_batch(self, tier: str, data_list: Iterable[SparkModel]) -> None:
        raise NotImplementedError("LakehouseAdapter.write_batch is a stub.")

    def read(self, tier: str, model: Type[SparkModel], filter_condition: Optional[str] = None) -> List[SparkModel]:
        raise NotImplementedError("LakehouseAdapter.read is a stub.")

    def read_dataframe(self, tier: str, table: str, filter_condition: Optional[str] = None):
        raise NotImplementedError("LakehouseAdapter.read_dataframe is a stub.")

    def update_records(self, tier: str, model_instance: SparkModel, fields_to_update: List[str]) -> None:
        raise NotImplementedError("LakehouseAdapter.update_records is a stub.")
