# Databricks notebook source
# MAGIC %md
# MAGIC # IDP Response Feature Serving Endpoint
# MAGIC
# MAGIC This notebook provisions a serving endpoint for the `idp_response` table.
# MAGIC Update the catalog/schema/table variables below and run the notebook.

# COMMAND ----------

# ---- Configuration (edit these) ----
CATALOG = "your_catalog"
SCHEMA = "your_schema"
TABLE = "idp_response"
ENDPOINT_NAME = "idp-response-serving"

# Optional: specify the primary key column
PRIMARY_KEY = "docuid"

# COMMAND ----------

full_table_name = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Ensure the table has a primary-key constraint
# MAGIC Databricks/Delta will store the constraint for documentation and data validation,
# MAGIC but note: primary keys are not physically enforced across all Delta configurations.
# MAGIC This still provides a schema contract that downstream systems can rely on.

# COMMAND ----------

try:
    spark.sql(
        f"""
        ALTER TABLE {full_table_name}
        ADD CONSTRAINT {TABLE}_{PRIMARY_KEY}_pk PRIMARY KEY ({PRIMARY_KEY})
        """
    )
    print("Primary key constraint added.")
except Exception as exc:
    print("Primary key constraint already exists or cannot be added:", exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Create or update a serving endpoint
# MAGIC This uses the Databricks SDK to create a serving endpoint.
# MAGIC If you are using Feature Store, replace the `entity_name` with a Feature Spec name.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

workspace = WorkspaceClient()

served_entity = ServedEntityInput(
    name="idp_response",
    entity_name=full_table_name,
    entity_version="1",
    workload_size="Small",
    workload_type="CPU",
)

config = EndpointCoreConfigInput(served_entities=[served_entity])

try:
    workspace.serving_endpoints.create(
        name=ENDPOINT_NAME,
        config=config,
    )
    print(f"Created serving endpoint: {ENDPOINT_NAME}")
except Exception as exc:
    print("Endpoint already exists or could not be created:", exc)
    try:
        workspace.serving_endpoints.update(
            name=ENDPOINT_NAME,
            config=config,
        )
        print(f"Updated serving endpoint: {ENDPOINT_NAME}")
    except Exception as update_exc:
        print("Failed to update endpoint:", update_exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Example query (optional)
# MAGIC You can query by docuid from the table directly if needed.

# COMMAND ----------

def fetch_by_docuid(docuid: str):
    return spark.table(full_table_name).filter(f"{PRIMARY_KEY} = '{docuid}'")

# Example:
# display(fetch_by_docuid("your_docuid"))
