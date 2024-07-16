import functools
from typing import Callable
from anystore.exceptions import DoesNotExist
from anystore.store import get_store
from anystore.util import make_signature_key


def anycache(func=None, **store_kwargs):
    key_func: Callable = store_kwargs.pop("key_func", make_signature_key)
    serialize_func: Callable = store_kwargs.pop("serialize_func", None)
    store = store_kwargs.pop("store", get_store(**store_kwargs))
    store = store.model_copy()
    store.raise_on_nonexist = True

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
                if serialize_func is not None:
                    res = serialize_func(res)
                if key is not None:
                    store.put(key, res)
                return res

        return _inner

    if func is None:
        return _decorator
    return _decorator(func)
