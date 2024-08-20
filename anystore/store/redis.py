"""
Store backend using redis-like stores such as Redis, Fakeredis or Apache Kvrocks
"""

from typing import Any, Generator, TYPE_CHECKING
from functools import cache

import redis

from anystore.exceptions import DoesNotExist
from anystore.logging import get_logger
from anystore.settings import Settings
from anystore.store.base import BaseStore
from anystore.types import Uri, Value

if TYPE_CHECKING:
    import fakeredis


log = get_logger(__name__)


@cache
def get_redis(uri: str) -> "fakeredis.FakeStrictRedis | redis.Redis":
    settings = Settings()
    if settings.redis_debug:
        import fakeredis

        con = fakeredis.FakeStrictRedis()
        con.ping()
        log.info("Redis connected: `fakeredis`")
        return con
    con = redis.from_url(uri)
    con.ping()
    log.info(f"Redis connected: `{uri}`")
    return con


class RedisStore(BaseStore):
    def _write(self, key: Uri, value: Value, **kwargs) -> None:
        con = get_redis(self.uri)
        con.set(str(key), value, **kwargs)

    def _read(self, key: Uri, raise_on_nonexist: bool | None = True, **kwargs) -> Any:
        con = get_redis(self.uri)
        key = str(key)
        # `None` could be stored as an actual value, to implement `raise_on_nonexist`
        # we need to check this first:
        if raise_on_nonexist and not con.exists(key):
            raise DoesNotExist
        res = con.get(str(key))
        # mimic fs read mode:
        if kwargs.get("mode") == "r" and isinstance(res, bytes):
            res = res.decode()
        return res

    def _exists(self, key: Uri) -> bool:
        con = get_redis(self.uri)
        res = con.exists(self.get_key(key))
        return bool(res)

    def _delete(self, key: Uri) -> None:
        con = get_redis(self.uri)
        con.delete(self.get_key(key))

    def _get_key_prefix(self) -> str:
        if self.backend_config is not None:
            return self.backend_config.get("redis_prefix") or "anystore"
        return "anystore"

    def _iterate_keys(self, prefix: str | None = None) -> Generator[str, None, None]:
        con = get_redis(self.uri)
        prefix = self.get_key(prefix or "") + "*"
        key_prefix = self._get_key_prefix()
        for key in con.scan_iter(prefix):
            key = key.decode()
            yield key[len(key_prefix) + 1 :]
