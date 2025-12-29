from functools import wraps
from typing import Any, Callable, Dict


class DependencyProvider:
    def __init__(self) -> None:
        self._context: Dict[str, Any] = {}

    def update_context(self, **kwargs: Any) -> None:
        self._context.update(kwargs)

    @property
    def context(self) -> Dict[str, Any]:
        return dict(self._context)


def log_message(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any):
        return func(*args, **kwargs)

    return wrapper


class LoggerWrapper:
    def log_message(self, func: Callable) -> Callable:
        return log_message(func)


DEPENDENCY_PROVIDER = DependencyProvider()
LOGGER = LoggerWrapper()
