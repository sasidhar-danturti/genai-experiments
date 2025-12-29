from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import uuid
from typing import ClassVar


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
