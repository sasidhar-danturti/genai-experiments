from __future__ import annotations

import os
import email
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, StructType


def _attachment_schema() -> ArrayType:
    return ArrayType(
        StructType(
            [
                StructField("attachment_name", StringType(), True),
                StructField("is_inline", BooleanType(), True),
            ]
        )
    )


def _extract_email_attachments(file_path: str, file_extension: str):
    try:
        if not file_extension or file_extension.lower() not in ("eml", "email", "msg"):
            return [{"attachment_name": None, "is_inline": False}]
        if not file_path or not os.path.exists(file_path):
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


def build_attachment_candidates(df: DataFrame) -> DataFrame:
    spark_df = df.withColumn(
        "file_extension",
        F.regexp_extract(F.col("file_path"), r"\\.([^.]+)$", 1),
    ).withColumn("file_name", F.element_at(F.split(F.col("file_path"), "/"), -1))

    extract_attachments_udf = F.udf(_extract_email_attachments, _attachment_schema())

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
