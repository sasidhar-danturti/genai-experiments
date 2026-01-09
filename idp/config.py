from dataclasses import dataclass


@dataclass
class AuditTables:
    batch: str = "batch"
    file_process: str = "file_process"
    task_execution: str = "task_execution"


@dataclass
class UnityCatalogConfig:
    uc_catalog_bronze: str = "bronze_catalog"
    uc_schema_bronze: str = "bronze_schema"
    uc_catalog_silver: str = "silver_catalog"
    uc_schema_silver: str = "silver_schema"
    uc_catalog_gold: str = "gold_catalog"
    uc_schema_gold: str = "gold_schema"


@dataclass
class DatabaseConfig:
    backend: str = "uc"


@dataclass
class Config:
    audit_tables: AuditTables = AuditTables()
    unity_catalog_config: UnityCatalogConfig = UnityCatalogConfig()
    database: DatabaseConfig = DatabaseConfig()

    def json(self) -> str:
        return (
            "{"
            f"\"audit_tables\":{self.audit_tables},"
            f"\"unity_catalog_config\":{self.unity_catalog_config},"
            f"\"database\":{self.database}"
            "}"
        )
