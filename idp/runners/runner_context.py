from dataclasses import dataclass
from typing import Optional, Any


@dataclass
class RunnerContext:
    batch_id: str
    task_id: str
    runner_name: str
    max_attempts: int = 3
    backoff_seconds: float = 1.0
    parallel: bool = True
    output_tier: str = "bronze"
    preferred_strategy: Optional[str] = None
    max_driver_records: int = 5000
    max_concurrency: int = 16
    partition_size: int = 2000
    worker_caps: Optional[Any] = None
