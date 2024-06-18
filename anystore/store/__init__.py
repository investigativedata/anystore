from functools import cache
from logging import getLogger
from urllib.parse import urlparse

from anystore.settings import Settings
from anystore.store.base import BaseStore
from anystore.store.fs import Store
from anystore.util import ensure_uri


log = getLogger(__name__)

settings = Settings()


@cache
def get_store(**kwargs) -> BaseStore:
    uri = kwargs.get("uri")
    if uri is None:
        if settings.yaml_uri is not None:
            store = BaseStore.from_yaml_uri(settings.yaml_uri, **kwargs)
            return get_store(**store.model_dump())
        if settings.json_uri is not None:
            store = BaseStore.from_json_uri(settings.json_uri, **kwargs)
            return get_store(**store.model_dump())
        uri = settings.uri
    uri = ensure_uri(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "redis":
        try:
            from anystore.store.redis import RedisStore

            return RedisStore(**kwargs)
        except ImportError:
            log.error("Install redis dependencies via `anystore[redis]`")
    if "sql" in parsed.scheme:
        try:
            from anystore.store.sql import SqlStore

            return SqlStore(**kwargs)
        except ImportError:
            log.error("Install sql dependencies via `anystore[sql]`")
    return Store(**kwargs)


__all__ = ["get_store", "Store", "RedisStore", "SqlStore"]
