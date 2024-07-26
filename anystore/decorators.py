import functools
from typing import Any, Callable
from anystore.exceptions import DoesNotExist
from anystore.store import get_store, BaseStore
from anystore.util import make_signature_key


def _setup_decorator(**kwargs) -> tuple[Callable, Callable, BaseStore]:
    key_func: Callable = kwargs.pop("key_func", make_signature_key)
    serialize_func: Callable = kwargs.pop("serialize_func", None)
    store = kwargs.pop("store", get_store(**kwargs))
    store = store.model_copy()
    store.raise_on_nonexist = True
    return key_func, serialize_func, store


def _handle_result(key: str, res: Any, serialize_func: Callable, store: BaseStore):
    if serialize_func is not None:
        res = serialize_func(res)
    if key is not None:
        store.put(key, res)
    return res


def anycache(func=None, **store_kwargs):
    key_func, serialize_func, store = _setup_decorator(**store_kwargs)

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
                return _handle_result(key, res, serialize_func, store)

        return _inner

    if func is None:
        return _decorator
    return _decorator(func)


def async_anycache(func=None, **store_kwargs):
    key_func, serialize_func, store = _setup_decorator(**store_kwargs)

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
                return _handle_result(key, res, serialize_func, store)

        return _inner

    if func is None:
        return _decorator
    return _decorator(func)
