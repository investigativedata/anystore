"""
Store backend using any file-like location usable via `fsspec`
"""

from datetime import datetime
from typing import Generator, BinaryIO

import fsspec
from banal import ensure_dict

from anystore.io import smart_open, smart_read, smart_stream, smart_write
from anystore.exceptions import DoesNotExist
from anystore.store.base import BaseStats, BaseStore
from anystore.types import Value, ValueStream
from anystore.util import join_uri


class Store(BaseStore):
    def __init__(self, **data):
        prefix = data.pop("prefix", None)
        if prefix:
            data["uri"] = join_uri(data["uri"], prefix)
        super().__init__(**data)
        self._fs = fsspec.url_to_fs(self.uri)[0]

    def _write(self, key: str, value: Value, **kwargs) -> None:
        kwargs.pop("ttl", None)
        smart_write(key, value, **kwargs)

    def _read(
        self, key: str, raise_on_nonexist: bool | None = True, **kwargs
    ) -> Value | None:
        try:
            return smart_read(key, **kwargs)
        except FileNotFoundError:
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    def _stream(
        self, key: str, raise_on_nonexist: bool | None = True, **kwargs
    ) -> ValueStream:
        try:
            yield from smart_stream(key, **kwargs)
        except FileNotFoundError:
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")

    def _exists(self, key: str) -> bool:
        return self._fs.exists(key)

    def _info(self, key: str) -> BaseStats:
        data = self._fs.info(key)
        ts = data.pop("created", None)
        data["updated_at"] = data.pop("LastModified", None)  # s3
        if ts:
            data["created_at"] = datetime.fromtimestamp(ts)
        return BaseStats(**data)

    def _delete(self, key: str) -> None:
        self._fs.delete(key)

    def _get_key_prefix(self) -> str:
        return str(self.uri).rstrip("/")

    def _bytes_io(self, key: str, **kwargs) -> BinaryIO:
        return smart_open(key, **kwargs)

    def _iterate_keys(self, prefix: str | None = None) -> Generator[str, None, None]:
        path = self.get_key(prefix or "")
        mapper = fsspec.get_mapper(path, **ensure_dict(self.backend_config))
        for key in mapper.keys():
            if prefix:
                key = f"{prefix}/{key}"
            yield key
