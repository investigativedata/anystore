from functools import cache
from urllib.parse import urlparse

from anystore.logging import get_logger
from anystore.settings import Settings
from anystore.store.base import BaseStore
from anystore.store.fs import Store
from anystore.store.memory import MemoryStore
from anystore.util import ensure_uri


log = get_logger(__name__)


@cache
def get_store(settings: Settings | None = None, **kwargs) -> BaseStore:
    settings = settings or Settings()
    uri = kwargs.pop("uri", None)
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
    if parsed.scheme == "memory":
        return MemoryStore(uri=uri, **kwargs)
    if parsed.scheme == "redis":
        try:
            from anystore.store.redis import RedisStore

            return RedisStore(uri=uri, **kwargs)
        except ImportError:
            log.error("Install redis dependencies via `anystore[redis]`")
    if "sql" in parsed.scheme:
        try:
            from anystore.store.sql import SqlStore

            return SqlStore(uri=uri, **kwargs)
        except ImportError:
            log.error("Install sql dependencies via `anystore[sql]`")
    return Store(uri=uri, **kwargs)


__all__ = ["get_store", "Store", "MemoryStore"]
