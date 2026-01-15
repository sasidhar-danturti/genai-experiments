from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import uuid
from typing import ClassVar, Optional


class SparkModel:
    __tablename__: ClassVar[str] = ""

    @classmethod
    def set_table_name(cls, name: str) -> None:
        cls.__tablename__ = name


class BatchState(Enum):
    START = "START"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"


class TaskExecutionState(Enum):
    START = "START"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class FileProcessState(Enum):
    DOWNLOAD_COMPLETED = "DOWNLOAD_COMPLETED"
    DOWNLOAD_FAILED = "DOWNLOAD_FAILED"


@dataclass
class Batch(SparkModel):
    config_json: str
    runner_name: str
    batch_id: str
    status: str
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TaskExecution(SparkModel):
    batch_id: str
    task_name: str
    status: str
    task_id: str
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class FileProcess(SparkModel):
    file_name: str
    batch_id: str
    task_id: str
    status: str
    error_message: str = ""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=datetime.utcnow)


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
    email_content: Optional[str]
    has_inline_attachments: bool
    inline_attachment_names: Optional[str]
    status: str
    batch_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class DownloadProcessOutput(SparkModel):
    __tablename__: ClassVar[str] = "download_process_output"

    id: str
    created_at: datetime
    updated_at: datetime
    file_path: str
    status: str
    repo: Optional[str]
    docuid: str
    batch_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class PageImageExtractionOutput(SparkModel):
    __tablename__: ClassVar[str] = "current_docs_with_page_images"

    docuid: str
    final_docuid: str
    attachment_name: Optional[str]
    downloaded_attachment_path: str
    file_extension: Optional[str]
    file_name: Optional[str]
    page_image: Optional[str]
    page_number: Optional[int]
    page_metadata_json: Optional[str]
    status: str
    batch_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class RoutePredictionOutput(SparkModel):
    __tablename__: ClassVar[str] = "current_routes_responses"

    docuid: str
    final_docuid: str
    attachment_name: Optional[str]
    downloaded_attachment_path: str
    file_extension: Optional[str]
    file_name: Optional[str]
    page_image: Optional[str]
    page_number: Optional[int]
    route_prediction_json: Optional[str]
    route_decision: Optional[str]
    status: str
    batch_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class PyMuPDFResponseOutput(SparkModel):
    __tablename__: ClassVar[str] = "current_pymupdf_responses"

    docuid: str
    final_docuid: str
    downloaded_attachment_path: str
    page_number: Optional[int]
    parser_response: Optional[str]
    parser_type: str
    status: str
    batch_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class ADIResponseOutput(SparkModel):
    __tablename__: ClassVar[str] = "current_adi_responses"

    docuid: str
    final_docuid: str
    file_name: Optional[str]
    page_number: Optional[int]
    parser_response: Optional[str]
    parser_type: str
    status: str
    batch_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class LLMResponseOutput(SparkModel):
    __tablename__: ClassVar[str] = "current_llm_responses"

    docuid: str
    final_docuid: str
    page_image: Optional[str]
    parser_response: Optional[str]
    parser_type: str
    status: str
    batch_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class ADILLMResponseOutput(SparkModel):
    __tablename__: ClassVar[str] = "current_adi_llm_responses"

    docuid: str
    final_docuid: str
    page_image: Optional[str]
    parser_response: Optional[str]
    parser_type: str
    status: str
    batch_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class NormalizedResponseOutput(SparkModel):
    __tablename__: ClassVar[str] = "current_normalised_responses"

    docuid: str
    final_docuid: Optional[str]
    parser_type: str
    parser_response: Optional[str]
    page_number: Optional[int]
    normalized_response_json: str
    status: str
    batch_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class DocPageOutput(SparkModel):
    __tablename__: ClassVar[str] = "current_extracted_pages"

    docuid: str
    parser_type: str
    source: str
    doc_kind: str
    normalized_response_json: str
    page_number: int
    extracted_page_text: str
    page_avg_conf: Optional[float]
    page_low_conf_words: int
    page_total_words: int
    standardized_page_text: str
    batch_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class DocTextOutput(SparkModel):
    __tablename__: ClassVar[str] = "current_extracted_doc_texts"

    docuid: str
    extracted_doc_text: str
    standardized_doc_text: str
    doc_avg_conf: Optional[float]
    doc_low_conf_words: int
    doc_total_words: int
    source: Optional[str]
    parser_type: Optional[str]
    doc_kind: Optional[str]
    normalized_response_json: Optional[str]
    batch_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class DocSummaryInputOutput(SparkModel):
    __tablename__: ClassVar[str] = "current_doc_summary_inputs"

    docuid: str
    summary_input_json: str
    doc_kind: Optional[str]
    source: Optional[str]
    parser_type: Optional[str]
    standardized_doc_text: str
    adi_structured_json: Optional[str]
    doc_avg_conf: Optional[float]
    doc_low_conf_words: int
    doc_total_words: int
    batch_id: Optional[str] = None
    task_id: Optional[str] = None
