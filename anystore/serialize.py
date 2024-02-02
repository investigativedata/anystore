import orjson
from typing import Any, Literal, TypeAlias

import cloudpickle
from pydantic import BaseModel


Mode: TypeAlias = Literal["auto", "raw", "pickle", "json"]


def to_store(value: Any, serialization_mode: Mode | None = "auto") -> bytes:
    mode = serialization_mode or "auto"
    if mode == "json":
        return orjson.dumps(value)
    if mode == "pickle":
        return cloudpickle.dumps(value)
    if mode == "raw":
        return value

    # auto
    if isinstance(value, BaseModel):
        return orjson.dumps(value.model_dump())
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    try:
        return orjson.dumps(value)
    except (orjson.JSONEncodeError, TypeError, ValueError):
        return cloudpickle.dumps(value)


def from_store(value: bytes, serialization_mode: Mode | None = "auto") -> Any:
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
            return value
