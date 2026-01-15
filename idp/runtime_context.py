from functools import wraps
from typing import Any, Callable, Dict


class DependencyProvider:
    def __init__(self) -> None:
        self._context: Dict[str, Any] = {}

    def set_context(self, context: Dict[str, Any]) -> None:
        self._context = dict(context)

    def update_context(self, **kwargs: Any) -> None:
        self._context.update(kwargs)

    def get_context(self) -> Dict[str, Any]:
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


def get_runtime_context() -> Dict[str, Any]:
    return DEPENDENCY_PROVIDER.get_context()


def set_runtime_context(context: Dict[str, Any]) -> None:
    DEPENDENCY_PROVIDER.set_context(context)
