from anystore.exceptions import DoesNotExist
from anystore.store import get_store
from anystore.util import make_signature_key


def anycache(**store_kwargs):
    key_func = store_kwargs.pop("key_func", make_signature_key)
    store = store_kwargs.pop("store", get_store(**store_kwargs))
    store = store.model_copy()
    store.raise_on_nonexist = True

    def _decorator(func):
        def _inner(*args, **kwargs):
            key = key_func(*args, **kwargs)
            try:
                return store.get(key)
            except DoesNotExist:
                res = func(*args, **kwargs)
                store.set(key, res)
                return res

        return _inner

    return _decorator
