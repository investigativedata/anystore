"""
Store backend using redis-like stores such as Redis, Fakeredis or Apache Kvrocks
"""

from typing import Any
import logging
import os
from functools import cache

from banal import as_bool
import redis
import fakeredis

from anystore.exceptions import DoesNotExist
from anystore.store.base import BaseStore
from anystore.types import Uri, Value


log = logging.getLogger(__name__)


@cache
def get_redis(uri: str) -> fakeredis.FakeStrictRedis | redis.Redis:
    if as_bool(os.environ.get("REDIS_DEBUG")):
        con = fakeredis.FakeStrictRedis()
        con.ping()
        log.info("Redis connected: `fakeredis`")
        return con
    con = redis.from_url(uri)
    con.ping()
    log.info("Redis connected: `{uri}`")
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

    def _get_key_prefix(self) -> str:
        return ""
