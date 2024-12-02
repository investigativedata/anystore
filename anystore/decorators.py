"""
Decorate functions to store results in a configurable cache and retrieve cached
results on next call.

Example:
    ```python
    from anystore import anycache

    @anycache(uri="./local-cache")
    def calculate(data):
        # a very time consuming task
        return result

    # 1. time: slow
    res = calculate(100)

    # 2. time: fast, as now cached
    res = calculate(100)
    ```

The cache key is computed based on the input arguments, but can be configured.

See below for reference details.
"""

import functools
from typing import Any, Callable

from pydantic import BaseModel

from anystore.exceptions import DoesNotExist
from anystore.serialize import Mode
from anystore.store import BaseStore, get_store
from anystore.util import make_signature_key


def _setup_decorator(**kwargs) -> tuple[Callable, BaseStore]:
    key_func: Callable = kwargs.pop("key_func", None) or make_signature_key
    store: BaseStore = kwargs.pop("store", None) or get_store(**kwargs)
    store = store.model_copy()
    store.model = kwargs.pop("model", None)
    store.default_ttl = kwargs.get("ttl") or store.default_ttl
    store.serialization_func = (
        kwargs.pop("serialization_func", None) or store.serialization_func
    )
    store.deserialization_func = (
        kwargs.pop("deserialization_func", None) or store.deserialization_func
    )
    store.raise_on_nonexist = True
    return key_func, store


def _handle_result(key: str, res: Any, store: BaseStore) -> Any:
    if store.serialization_func is not None:
        res = store.serialization_func(res)
    if key:
        store.put(key, res, serialization_func=lambda x: x)  # already serialized
    return res


def anycache(
    func: Callable[..., Any] | None = None,
    store: BaseStore | None = None,
    model: BaseModel | None = None,
    key_func: Callable[..., str | None] | None = None,
    serialization_mode: Mode | None = "auto",
    serialization_func: Callable | None = None,
    deserialization_func: Callable | None = None,
    ttl: int | None = None,
    **store_kwargs: Any
) -> Callable[..., Any]:
    """
    Cache a function call in a configurable cache backend. By default, the
    default store is used (configured via environment)

    Example:
        ```python
        @anycache(
            store=get_store("redis://localhost"),
            key_func=lambda *args, **kwargs: args[0].upper()
        )
        def compute(*args, **kwargs):
            return "result"
        ```

    Note:
        If the `key_func` returns `None` as the computed cache key, the result
        will not be cached (this can be used to dynamically disable caching
        based on function input)

    See [`anystore.serialize`][anystore.serialize] for serialization reference.

    Args:
        func: The function to wrap
        serialization_mode: "auto", "pickle", "json", "raw"
        serialization_func: Function to use to serialize
        deserialization_func: Function to use to deserialize, takes bytes as input
        model: Pydantic model to use for serialization from a json bytes string
        key_func: Function to compute the cache key
        ttl: Key ttl for supported backends
        **store_kwargs: Any other store options or backend specific
            configuration to pass through

    Returns:
        Callable: The decorated function
    """
    key_func, store = _setup_decorator(
        store=store,
        key_func=key_func,
        serialization_mode=serialization_mode,
        serialization_func=serialization_func,
        deserialization_func=deserialization_func,
        model=model,
        ttl=ttl,
        **store_kwargs
    )

    def _decorator(func):
        @functools.wraps(func)
        def _inner(*args, **kwargs):
            key = key_func(*args, **kwargs)
            try:
                if key is not None:
                    return store.get(key)
                raise DoesNotExist
            except DoesNotExist:
                res = func(*args, **kwargs)
                return _handle_result(key, res, store)

        return _inner

    if func is None:
        return _decorator
    return _decorator(func)


def async_anycache(func=None, **store_kwargs):
    """
    Async implementation of the [@anycache][anystore.decorators.anycache]
    decorator
    """
    key_func, store = _setup_decorator(**store_kwargs)

    def _decorator(func):
        @functools.wraps(func)
        async def _inner(*args, **kwargs):
            key = key_func(*args, **kwargs)
            try:
                if key is not None:
                    return store.get(key)
                raise DoesNotExist
            except DoesNotExist:
                res = await func(*args, **kwargs)
                return _handle_result(key, res, store)

        return _inner

    if func is None:
        return _decorator
    return _decorator(func)
