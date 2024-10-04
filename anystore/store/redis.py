"""
Store backend using redis-like stores such as Redis, Fakeredis or Apache Kvrocks
"""

from functools import cache
from typing import Any, Generator, BinaryIO
from io import BytesIO

import fakeredis
import redis

from anystore.exceptions import DoesNotExist
from anystore.logging import get_logger
from anystore.settings import Settings
from anystore.store.base import BaseStore, BaseStats, VirtualIOMixin
from anystore.types import Value
from anystore.util import join_relpaths


log = get_logger(__name__)


@cache
def get_redis(uri: str) -> fakeredis.FakeStrictRedis | redis.Redis:
    settings = Settings()
    if settings.redis_debug:
        con = fakeredis.FakeStrictRedis()
        con.ping()
        log.info("Redis connected: `fakeredis`")
        return con
    con = redis.from_url(uri)
    con.ping()
    log.info(f"Redis connected: `{uri}`")
    return con


class RedisStore(VirtualIOMixin, BaseStore):
    def __init__(self, **data):
        super().__init__(**data)
        self._con = get_redis(self.uri)

    def _write(self, key: str, value: Value, **kwargs) -> None:
        ttl = kwargs.pop("ttl", None) or None
        kwargs.pop("mode", None)
        self._con.set(key, value, ex=ttl, **kwargs)

    def _read(self, key: str, raise_on_nonexist: bool | None = True, **kwargs) -> Any:
        # `None` could be stored as an actual value, to implement `raise_on_nonexist`
        # we need to check this first:
        if raise_on_nonexist and not self._con.exists(key):
            raise DoesNotExist
        res = self._con.get(key)
        # mimic fs read mode:
        if kwargs.get("mode") == "r" and isinstance(res, bytes):
            res = res.decode()
        return res

    def _exists(self, key: str) -> bool:
        res = self._con.exists(key)
        return bool(res)

    def _info(self, key: str) -> BaseStats:
        data = self._read(key)
        return BaseStats(size=len(data))

    def _delete(self, key: str) -> None:
        self._con.delete(key)

    def _get_key_prefix(self) -> str:
        if self.backend_config is not None:
            return self.backend_config.get("redis_prefix") or "anystore"
        return "anystore"

    def _iterate_keys(
        self,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        glob: str | None = None,
    ) -> Generator[str, None, None]:
        prefix = self.get_key(prefix or "")
        prefix = join_relpaths(prefix, glob or "*")
        key_prefix = self._get_key_prefix()
        for key in self._con.scan_iter(prefix):
            key = key.decode()
            key = key[len(key_prefix) + 1 :]
            if not exclude_prefix or not key.startswith(exclude_prefix):
                yield key
