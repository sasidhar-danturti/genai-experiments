from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, StructType

from idp.db_manager.spark_models import (
    AttachmentExtractionInput,
    AttachmentExtractionOutput,
)
from idp.runners.attachment_extraction_worker import AttachmentExtractionWorker
from idp.runners.base_runner import BaseRunner


class AttachmentExtractionRunner(BaseRunner):
    input_model = AttachmentExtractionInput
    output_model = AttachmentExtractionOutput
    input_table = "current_downloaded_docs"
    history_table = "current_docs_with_attachments"

    def __init__(self, adapter=None, planner=None, is_first: bool = True, is_last: bool = True) -> None:
        super().__init__(
            worker=AttachmentExtractionWorker(),
            adapter=adapter,
            planner=planner,
            is_first=is_first,
            is_last=is_last,
        )

    def _attachment_schema(self) -> ArrayType:
        return ArrayType(
            StructType(
                [
                    StructField("attachment_name", StringType(), True),
                    StructField("is_inline", BooleanType(), True),
                ]
            )
        )

    def _extract_email_attachments(self, file_path: str, file_extension: str):
        try:
            if not file_extension or file_extension.lower() not in ("eml", "email", "msg"):
                return [{"attachment_name": None, "is_inline": False}]
            if not file_path:
                return [{"attachment_name": None, "is_inline": False}]
            import os
            import email

            if not os.path.exists(file_path):
                return [{"attachment_name": None, "is_inline": False}]
            with open(file_path, "rb") as handle:
                content_bytes = handle.read()
            message = email.message_from_bytes(content_bytes)
            attachments = [{"attachment_name": None, "is_inline": False}]
            for part in message.walk():
                content_disposition = part.get("Content-Disposition", "")
                if part.get_content_maintype() == "multipart":
                    continue
                if "attachment" in content_disposition or "inline" in content_disposition:
                    name = part.get_filename() or "unknown"
                    attachments.append(
                        {
                            "attachment_name": name,
                            "is_inline": "inline" in content_disposition,
                        }
                    )
            return attachments
        except Exception:
            return [{"attachment_name": None, "is_inline": False}]

    def _build_candidates(self, df: DataFrame) -> DataFrame:
        spark_df = df.withColumn(
            "file_extension",
            F.regexp_extract(F.col("file_path"), r"\\.([^.]+)$", 1),
        ).withColumn("file_name", F.element_at(F.split(F.col("file_path"), "/"), -1))

        extract_attachments_udf = F.udf(
            self._extract_email_attachments,
            self._attachment_schema(),
        )

        with_attachments = spark_df.withColumn(
            "attachments",
            extract_attachments_udf(F.col("file_path"), F.col("file_extension")),
        )

        exploded_df = with_attachments.select(
            "*",
            F.posexplode_outer("attachments").alias("attachment_index", "attachment"),
        ).drop("attachments")

        final_df = (
            exploded_df.withColumn(
                "final_docuid",
                F.when(
                    F.col("attachment_index") == 0,
                    F.col("docuid"),
                ).otherwise(
                    F.concat_ws(
                        "_",
                        F.col("docuid"),
                        F.col("attachment_index").cast("string"),
                    )
                ),
            )
            .withColumn("attachment_name", F.col("attachment.attachment_name"))
            .withColumn("is_inline", F.col("attachment.is_inline"))
            .drop("attachment")
        )

        return final_df

    def run(self, df_or_records: DataFrame = None, ctx_overrides: dict | None = None):
        if df_or_records is None:
            df_or_records = self.load_inputs_dataframe()
        candidates = self._build_candidates(df_or_records)
        overrides = {"preferred_strategy": "spark_partition"}
        if ctx_overrides:
            overrides.update(ctx_overrides)
        return super().run(candidates, ctx_overrides=overrides)
