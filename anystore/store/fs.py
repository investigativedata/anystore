"""
Store backend using any file-like location usable via `fsspec`
"""

from typing import Generator

import fsspec
from banal import ensure_dict

from anystore.io import smart_read, smart_stream, smart_write
from anystore.exceptions import DoesNotExist
from anystore.store.base import BaseStore
from anystore.types import Value


class Store(BaseStore):
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
    ) -> Generator[Value, None, None]:
        try:
            yield from smart_stream(key, **kwargs)
        except FileNotFoundError:
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")

    def _exists(self, key: str) -> bool:
        fs = fsspec.filesystem(self.scheme)
        return fs.exists(key)

    def _delete(self, key: str) -> None:
        fs = fsspec.filesystem(self.scheme)
        fs.delete(key)

    def _get_key_prefix(self) -> str:
        return str(self.uri).rstrip("/")

    def _iterate_keys(self, prefix: str | None = None) -> Generator[str, None, None]:
        path = self.get_key(prefix or "")
        mapper = fsspec.get_mapper(path, **ensure_dict(self.backend_config))
        for key in mapper.keys():
            if prefix:
                key = f"{prefix}/{key}"
            yield key
