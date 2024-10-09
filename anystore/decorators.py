import functools
from typing import Callable, Any
from anystore.exceptions import DoesNotExist
from anystore.store import get_store, BaseStore
from anystore.util import make_signature_key


def _setup_decorator(**kwargs) -> tuple[Callable, BaseStore]:
    key_func: Callable = kwargs.pop("key_func", make_signature_key)
    store: BaseStore = kwargs.pop("store", get_store(**kwargs))
    store = store.model_copy()
    store.serialization_func = (
        kwargs.pop("serialization_func", None) or store.serialization_func
    )
    store.deserialization_func = (
        kwargs.pop("deserialization_func", None) or store.deserialization_func
    )
    store.raise_on_nonexist = True
    return key_func, store


def _handle_result(key: str, res: Any, store: BaseStore):
    if store.serialization_func is not None:
        res = store.serialization_func(res)
    if key:
        store.put(key, res, serialization_func=lambda x: x)  # already serialized
    return res


def anycache(func=None, **store_kwargs):
    key_func, store = _setup_decorator(**store_kwargs)

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
