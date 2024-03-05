"""
Store backend using any file-like location usable via `fsspec`
"""

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

    def _get_key_prefix(self) -> str:
        return str(self.uri)
