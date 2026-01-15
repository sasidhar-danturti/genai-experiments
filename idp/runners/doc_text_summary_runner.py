from __future__ import annotations

import json
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from idp.db_manager.spark_models import DocPageOutput, DocSummaryInputOutput, DocTextOutput
from idp.runners.aggregation_runner import AggregationRunner
from idp.runners.normalization_helpers import extract_adi_structured


class DocTextSummaryRunner(AggregationRunner):
    input_table = DocPageOutput.__tablename__
    output_tables = (
        DocTextOutput.__tablename__,
        DocSummaryInputOutput.__tablename__,
    )

    def build_outputs(self, df_or_records: DataFrame) -> dict[str, DataFrame]:
        per_page_df = (
            df_or_records.groupBy("docuid", "page_number")
            .agg(
                F.first("extracted_page_text", ignorenulls=True).alias(
                    "extracted_page_text"
                ),
                F.first("standardized_page_text", ignorenulls=True).alias(
                    "standardized_page_text"
                ),
                F.first("normalized_response_json", ignorenulls=True).alias(
                    "normalized_response_json"
                ),
                F.first("page_avg_conf", ignorenulls=True).alias("page_avg_conf"),
                F.first("page_low_conf_words", ignorenulls=True).alias(
                    "page_low_conf_words"
                ),
                F.first("page_total_words", ignorenulls=True).alias("page_total_words"),
                F.first("source", ignorenulls=True).alias("source"),
                F.first("parser_type", ignorenulls=True).alias("parser_type"),
                F.first("doc_kind", ignorenulls=True).alias("doc_kind"),
            )
        )

        doc_text_df = (
            per_page_df.groupBy("docuid")
            .agg(
                F.sort_array(
                    F.collect_list(F.struct("page_number", "extracted_page_text"))
                ).alias("ex_pages"),
                F.sort_array(
                    F.collect_list(F.struct("page_number", "standardized_page_text"))
                ).alias("std_pages"),
                F.sort_array(
                    F.collect_list(F.struct("page_number", "normalized_response_json"))
                ).alias("norm_pages"),
                F.avg("page_avg_conf").alias("doc_avg_conf"),
                F.sum("page_low_conf_words").alias("doc_low_conf_words"),
                F.sum("page_total_words").alias("doc_total_words"),
                F.first("source", ignorenulls=True).alias("source"),
                F.first("parser_type", ignorenulls=True).alias("parser_type"),
                F.first("doc_kind", ignorenulls=True).alias("doc_kind"),
            )
            .withColumn(
                "extracted_doc_text",
                F.to_json(
                    F.map_from_entries(
                        F.transform(
                            F.col("ex_pages"),
                            lambda x: F.struct(
                                x.page_number.cast("string"),
                                x.extracted_page_text,
                            ),
                        )
                    )
                ),
            )
            .withColumn(
                "standardized_doc_text",
                F.to_json(
                    F.map_from_entries(
                        F.transform(
                            F.col("std_pages"),
                            lambda x: F.struct(
                                x.page_number.cast("string"),
                                x.standardized_page_text,
                            ),
                        )
                    )
                ),
            )
            .withColumn(
                "normalized_response_json",
                F.to_json(
                    F.map_from_entries(
                        F.transform(
                            F.col("norm_pages"),
                            lambda x: F.struct(
                                x.page_number.cast("string"),
                                x.normalized_response_json,
                            ),
                        )
                    )
                ),
            )
            .drop("ex_pages", "std_pages", "norm_pages")
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
        return {
            DocTextOutput.__tablename__: doc_text_df,
            DocSummaryInputOutput.__tablename__: summary_input_df,
        }

    def run(self, df_or_records: DataFrame = None) -> dict[str, DataFrame]:
        return super().run(df_or_records)
