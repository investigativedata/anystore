import orjson
from typing import Any, Callable, Literal, TypeAlias

import cloudpickle
from pydantic import BaseModel

from anystore.types import Model


Mode: TypeAlias = Literal["auto", "raw", "pickle", "json"]


def to_store(
    value: Any,
    serialization_mode: Mode | None = "auto",
    serialization_func: Callable | None = None,
    model: Model | None = None,
) -> bytes:
    if model is not None:
        return value.model_dump_json().encode()
    if serialization_func is not None:
        value = serialization_func(value)

    mode = serialization_mode or "auto"
    if mode == "json":
        return orjson.dumps(value)
    if mode == "pickle":
        return cloudpickle.dumps(value)
    if mode == "raw":
        return value

    # auto
    if isinstance(value, BaseModel):
        return value.model_dump_json().encode()
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    try:
        return orjson.dumps(value)
    except (orjson.JSONEncodeError, TypeError, ValueError):
        return cloudpickle.dumps(value)


def from_store(
    value: bytes,
    serialization_mode: Mode | None = "auto",
    deserialization_func: Callable | None = None,
    model: Model | None = None,
) -> Any:
    if model is not None:
        data = orjson.loads(value)
        return model(**data)
    if deserialization_func is not None:
        value = deserialization_func(value)

    mode = serialization_mode or "auto"
    if mode == "raw":
        return value
    if mode == "pickle":
        return cloudpickle.loads(value)
    if mode == "json":
        return orjson.loads(value)

    # auto
    try:
        return orjson.loads(value)
    except (orjson.JSONDecodeError, TypeError, ValueError):
        try:
            return cloudpickle.loads(value)
        except Exception:
            if isinstance(value, bytes):
                try:
                    return value.decode()
                except UnicodeDecodeError:
                    pass
            return value
