from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Optional

from idp.db_manager.spark_models import SparkModel


@dataclass
class AttachmentExtractionInput(SparkModel):
    __tablename__: ClassVar[str] = "current_downloaded_docs"

    docuid: str
    file_path: str
    file_extension: str
    file_name: str
    attachment_index: int
    attachment_name: Optional[str]
    is_inline: bool
    final_docuid: str


@dataclass
class AttachmentExtractionOutput(SparkModel):
    __tablename__: ClassVar[str] = "current_docs_with_attachments"

    docuid: str
    final_docuid: str
    file_path: str
    file_extension: str
    file_name: str
    attachment_index: int
    attachment_name: Optional[str]
    is_inline: bool
    downloaded_attachment_path: str
    status: str
    batch_id: Optional[str] = None
    task_id: Optional[str] = None
