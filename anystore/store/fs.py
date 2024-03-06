"""
Store backend using any file-like location usable via `fsspec`
"""

from typing import Generator

import fsspec
from banal import ensure_dict

from anystore.io import smart_read, smart_write
from anystore.exceptions import DoesNotExist
from anystore.store.base import BaseStore
from anystore.types import Uri, Value


class Store(BaseStore):
    def _write(self, key: Uri, value: Value, **kwargs) -> None:
        return smart_write(key, value, **kwargs)

    def _read(
        self, key: Uri, raise_on_nonexist: bool | None = True, **kwargs
    ) -> Value | None:
        try:
            return smart_read(str(key), **kwargs)
        except FileNotFoundError:
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    # def _exists(self, key: Uri) -> bool:
    def _get_key_prefix(self) -> str:
        return str(self.uri).rstrip("/")

    def _iterate_keys(self, prefix: str | None = None) -> Generator[str, None, None]:
        path = self.get_key(prefix or "")
        mapper = fsspec.get_mapper(path, **ensure_dict(self.backend_config))
        for key in mapper.keys():
            if prefix:
                key = f"{prefix}/{key}"
            yield key
