from __future__ import annotations

import json
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from idp.db_manager.spark_models import DocPageOutput, DocSummaryInputOutput, DocTextOutput
from idp.runners.normalization_helpers import extract_adi_structured


class DocTextSummaryRunner:
    def __init__(self, spark, adapter) -> None:
        self.spark = spark
        self.adapter = adapter

    def load_inputs_dataframe(self) -> DataFrame:
        return self.adapter.read_dataframe("bronze", DocPageOutput.__tablename__)

    def run(self, df_or_records: DataFrame = None) -> None:
        if df_or_records is None:
            df_or_records = self.load_inputs_dataframe()

        doc_text_df = (
            df_or_records.groupBy("docuid")
            .agg(
                F.sort_array(
                    F.collect_list(F.struct("page_number", "extracted_page_text"))
                ).alias("ex_pages"),
                F.sort_array(
                    F.collect_list(F.struct("page_number", "standardized_page_text"))
                ).alias("std_pages"),
                F.avg("page_avg_conf").alias("doc_avg_conf"),
                F.sum("page_low_conf_words").alias("doc_low_conf_words"),
                F.sum("page_total_words").alias("doc_total_words"),
                F.first("source", ignorenulls=True).alias("source"),
                F.first("parser_type", ignorenulls=True).alias("parser_type"),
                F.first("doc_kind", ignorenulls=True).alias("doc_kind"),
                F.first("normalized_response_json", ignorenulls=True).alias(
                    "normalized_response_json"
                ),
            )
            .withColumn(
                "extracted_doc_text",
                F.expr(
                    """
                  concat_ws(
                    '\\n\\n',
                    transform(ex_pages, x -> concat('--- PAGE ', cast(x.page_number as string), ' ---\\n', x.extracted_page_text))
                  )
                """
                ),
            )
            .withColumn(
                "standardized_doc_text",
                F.expr(
                    """
                  concat_ws(
                    '\\n\\n',
                    transform(std_pages, x -> concat('--- PAGE ', cast(x.page_number as string), ' ---\\n', x.standardized_page_text))
                  )
                """
                ),
            )
            .drop("ex_pages", "std_pages")
        )

        doc_text_df.write.format("delta").mode("overwrite").saveAsTable(
            f"{self.adapter.qualified_table('bronze', DocTextOutput.__tablename__)}"
        )

        extract_adi_structured_udf = F.udf(extract_adi_structured, "string")
        summary_input_df = (
            doc_text_df.withColumn(
                "adi_structured_json",
                extract_adi_structured_udf("normalized_response_json"),
            )
            .withColumn(
                "summary_input_json",
                F.to_json(
                    F.struct(
                        F.col("docuid"),
                        F.col("doc_kind"),
                        F.col("source"),
                        F.col("parser_type"),
                        F.col("standardized_doc_text").alias("text"),
                        F.col("adi_structured_json").alias("structured"),
                        F.struct(
                            F.col("doc_avg_conf"),
                            F.col("doc_low_conf_words"),
                            F.col("doc_total_words"),
                        ).alias("quality"),
                    )
                ),
            )
        )

        summary_input_df.write.format("delta").mode("overwrite").saveAsTable(
            f"{self.adapter.qualified_table('bronze', DocSummaryInputOutput.__tablename__)}"
        )
