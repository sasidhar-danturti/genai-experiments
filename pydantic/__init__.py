"""Lightweight fallback implementation of the Pydantic API used in tests.

This shim is intentionally minimal and only implements the pieces of the
``pydantic`` package that are exercised by the local schemas.  It is *not* a
complete drop-in replacement for the real library, but it provides a compatible
surface for the small subset of functionality that we rely on in tests.  If the
real dependency is available it will shadow this module instead.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional

__all__ = ["BaseModel", "Field", "ConfigDict"]


_UNSET = object()


@dataclass
class _FieldInfo:
    """Stores metadata about a model field."""

    default: Any = _UNSET
    default_factory: Optional[Callable[[], Any]] = None

    def get_default(self) -> Any:
        if self.default is not _UNSET:
            return self.default
        if self.default_factory is not None:
            return self.default_factory()
        return _UNSET


def Field(*, default: Any = _UNSET, default_factory: Optional[Callable[[], Any]] = None) -> _FieldInfo:
    """Mimic :func:`pydantic.Field` for the limited features we use."""

    if default is not _UNSET and default_factory is not None:
        raise TypeError("Cannot set both default and default_factory")
    return _FieldInfo(default=default, default_factory=default_factory)


def ConfigDict(**kwargs: Any) -> Dict[str, Any]:
    """Return the provided keyword arguments as a configuration dictionary."""

    return dict(kwargs)


class BaseModel:
    """Simplified ``BaseModel`` supporting basic validation-free behaviour."""

    __model_fields__: Dict[str, _FieldInfo] = {}
    __allow_mutation__: bool = True
    __use_enum_values__: bool = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        annotations = getattr(cls, "__annotations__", {})
        fields: Dict[str, _FieldInfo] = {}
        allow_mutation_default = True
        use_enum_default = False
        for base in reversed(cls.__mro__[1:]):
            base_fields = getattr(base, "__model_fields__", None)
            if base_fields:
                fields.update({name: info for name, info in base_fields.items()})
            if hasattr(base, "__allow_mutation__"):
                allow_mutation_default = bool(getattr(base, "__allow_mutation__"))
            if hasattr(base, "__use_enum_values__"):
                use_enum_default = bool(getattr(base, "__use_enum_values__"))
        for name, _ in annotations.items():
            attribute = getattr(cls, name, _UNSET)
            if isinstance(attribute, _FieldInfo):
                field_info = attribute
                if name in cls.__dict__:
                    delattr(cls, name)
            else:
                field_info = _FieldInfo(default=attribute)
            fields[name] = field_info
        cls.__model_fields__ = fields

        config: Dict[str, Any] = {}
        if hasattr(cls, "model_config"):
            config_source = getattr(cls, "model_config")
            if isinstance(config_source, dict):
                config.update(config_source)
        if hasattr(cls, "Config"):
            config.update(
                {
                    key: getattr(cls.Config, key)
                    for key in dir(cls.Config)
                    if not key.startswith("_") and not callable(getattr(cls.Config, key))
                }
            )
        allow_mutation = config.get("allow_mutation")
        frozen = config.get("frozen")
        if frozen is not None:
            cls.__allow_mutation__ = not bool(frozen)
        elif allow_mutation is not None:
            cls.__allow_mutation__ = bool(allow_mutation)
        else:
            cls.__allow_mutation__ = allow_mutation_default
        cls.__use_enum_values__ = bool(config.get("use_enum_values", use_enum_default))

    def __init__(self, **data: Any) -> None:
        for name, info in self.__model_fields__.items():
            if name in data:
                value = data.pop(name)
            else:
                value = info.get_default()
                if value is _UNSET:
                    raise TypeError(f"Missing required field '{name}'")
            object.__setattr__(self, name, value)
        if data:
            unexpected = ", ".join(sorted(data))
            raise TypeError(f"Unexpected fields: {unexpected}")

    def __setattr__(self, key: str, value: Any) -> None:
        if not self.__class__.__allow_mutation__ and key in self.__model_fields__:
            raise AttributeError(f"Cannot modify field '{key}' on immutable model")
        object.__setattr__(self, key, value)

    def model_dump(self, *, exclude_none: bool = False) -> Dict[str, Any]:
        return self._serialize_model(exclude_none=exclude_none)

    def dict(self, *_, exclude_none: bool = False, **__) -> Dict[str, Any]:
        return self._serialize_model(exclude_none=exclude_none)

    def model_copy(self, *, update: Optional[Dict[str, Any]] = None) -> "BaseModel":
        data = {name: getattr(self, name) for name in self.__model_fields__}
        if update:
            data.update(update)
        return self.__class__(**data)

    def copy(self, *, update: Optional[Dict[str, Any]] = None) -> "BaseModel":  # pragma: no cover - legacy API
        return self.model_copy(update=update)

    def _serialize_model(self, *, exclude_none: bool) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        for name in self.__model_fields__:
            value = getattr(self, name)
            if exclude_none and value is None:
                continue
            payload[name] = self._serialize_value(value, exclude_none=exclude_none)
        return payload

    def _serialize_value(self, value: Any, *, exclude_none: bool) -> Any:
        if isinstance(value, BaseModel):
            return value._serialize_model(exclude_none=exclude_none)
        if isinstance(value, list):
            return [self._serialize_value(item, exclude_none=exclude_none) for item in value]
        if isinstance(value, dict):
            items = (
                (k, self._serialize_value(v, exclude_none=exclude_none))
                for k, v in value.items()
                if not (exclude_none and v is None)
            )
            return {k: v for k, v in items}
        if isinstance(value, Enum) and self.__class__.__use_enum_values__:
            return value.value
        return value

    @classmethod
    def model_rebuild(cls) -> None:
        return None

    @classmethod
    def update_forward_refs(cls, **_: Any) -> None:
        return None

    def __repr__(self) -> str:
        field_values = ", ".join(f"{name}={getattr(self, name)!r}" for name in self.__model_fields__)
        return f"{self.__class__.__name__}({field_values})"


