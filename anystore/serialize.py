import orjson
from typing import Any

import cloudpickle


def to_cloud(value: Any, use_pickle: bool | None = True) -> Any:
    if use_pickle:
        return cloudpickle.dumps(value)
    try:
        return orjson.dumps(value)
    except TypeError:
        return value


def from_cloud(value: Any, use_pickle: bool | None = True) -> Any:
    if use_pickle:
        return cloudpickle.loads(value)
    try:
        return orjson.loads(value)
    except orjson.JSONDecodeError:
        return value
