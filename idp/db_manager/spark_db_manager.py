from typing import Iterable, List, Optional, Type
from idp.db_manager.spark_models import SparkModel


class SparkDBManager:
    def write(self, catalog: str, schema: str, data: SparkModel) -> None:
        raise NotImplementedError("SparkDBManager.write is a stub.")

    def write_batch(
        self,
        catalog: str,
        schema: str,
        data_list: Iterable[SparkModel],
        mode: str = "append",
    ) -> None:
        raise NotImplementedError("SparkDBManager.write_batch is a stub.")

    def read(self, catalog: str, schema: str, model: Type[SparkModel], filter_condition: Optional[str] = None) -> List[SparkModel]:
        raise NotImplementedError("SparkDBManager.read is a stub.")

    def read_dataframe(self, catalog: str, schema: str, table: str, filter_condition: Optional[str] = None):
        raise NotImplementedError("SparkDBManager.read_dataframe is a stub.")

    def update_records(self, catalog: str, schema: str, model_instance: SparkModel, fields_to_update: List[str]) -> None:
        raise NotImplementedError("SparkDBManager.update_records is a stub.")
