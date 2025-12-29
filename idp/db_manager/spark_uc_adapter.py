from typing import Iterable, List, Optional, Type
from idp.db_manager.db_adapter import DBAdapter
from idp.db_manager.spark_db_manager import SparkDBManager
from idp.db_manager.spark_models import SparkModel


class SparkUCAdapter(DBAdapter):
    def __init__(self, config):
        self.config = config
        self.sp_obj = SparkDBManager()
        self.tiers = {
            "bronze": (config.unity_catalog_config.uc_catalog_bronze, config.unity_catalog_config.uc_schema_bronze),
            "silver": (config.unity_catalog_config.uc_catalog_silver, config.unity_catalog_config.uc_schema_silver),
            "gold": (config.unity_catalog_config.uc_catalog_gold, config.unity_catalog_config.uc_schema_gold),
        }

    def _catalog_schema(self, tier: str):
        if tier not in self.tiers:
            raise ValueError(f"Unknown tier: {tier}")
        return self.tiers[tier]

    def qualified_table(self, tier: str, table: str) -> str:
        catalog, schema = self._catalog_schema(tier)
        return f"{catalog}.{schema}.{table}"

    def write(self, tier: str, data: SparkModel) -> None:
        catalog, schema = self._catalog_schema(tier)
        self.sp_obj.write(catalog=catalog, schema=schema, data=data)

    def write_batch(self, tier: str, data_list: Iterable[SparkModel]) -> None:
        catalog, schema = self._catalog_schema(tier)
        self.sp_obj.write_batch(catalog=catalog, schema=schema, data_list=list(data_list))

    def read(self, tier: str, model: Type[SparkModel], filter_condition: Optional[str] = None) -> List[SparkModel]:
        catalog, schema = self._catalog_schema(tier)
        return self.sp_obj.read(catalog=catalog, schema=schema, model=model, filter_condition=filter_condition)

    def read_dataframe(self, tier: str, table: str, filter_condition: Optional[str] = None):
        catalog, schema = self._catalog_schema(tier)
        return self.sp_obj.read_dataframe(catalog=catalog, schema=schema, table=table, filter_condition=filter_condition)

    def update_records(self, tier: str, model_instance: SparkModel, fields_to_update: List[str]) -> None:
        catalog, schema = self._catalog_schema(tier)
        self.sp_obj.update_records(catalog=catalog, schema=schema, model_instance=model_instance, fields_to_update=fields_to_update)
