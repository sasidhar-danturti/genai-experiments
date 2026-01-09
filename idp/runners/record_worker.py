from abc import ABC, abstractmethod
from typing import Any


class RecordWorker(ABC):
    is_spark_serializable: bool = True
    is_threadsafe: bool = True
    supports_batch: bool = False

    @abstractmethod
    def process(self, record: Any) -> Any:
        ...
