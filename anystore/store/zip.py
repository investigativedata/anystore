"""
Use a zip file as a store backend. Read-only for remote sources, writeable locally
"""

import contextlib
from typing import BinaryIO, Any, Generator, Literal
from fsspec.implementations.zip import ZipFileSystem

from anystore.exceptions import DoesNotExist
from anystore.io import DEFAULT_MODE, DEFAULT_WRITE_MODE
from anystore.store.base import BaseStats, BaseStore
from anystore.types import Value, ValueStream, Uri


class ZipStore(BaseStore):
    def __init__(self, **data):
        super().__init__(**data)
        self.__exists = None

    @contextlib.contextmanager
    def _get_handler(self, mode: Literal["r", "a"]) -> Any:
        if self.__exists is None:
            try:
                handler = ZipFileSystem(str(self.uri), mode)
            except FileNotFoundError:
                # create initial file
                ZipFileSystem(str(self.uri), "w").close()
                self.__exists = True
                handler = ZipFileSystem(str(self.uri), mode)
        else:
            handler = ZipFileSystem(str(self.uri), mode)
        try:
            yield handler
        finally:
            handler.close()

    @contextlib.contextmanager
    def _reader(self, key: str, **kwargs) -> Any:
        with self._get_handler("r") as reader:
            handler = reader.open(key, **kwargs)
            try:
                yield handler
            finally:
                handler.close()

    @contextlib.contextmanager
    def _writer(self, key: str, **kwargs) -> Any:
        with self._get_handler("a") as writer:
            handler = writer.open(key, **kwargs)
            try:
                yield handler
            finally:
                handler.close()

    def _write(self, key: str, value: Value, **kwargs) -> None:
        kwargs["mode"] = kwargs.pop("mode", DEFAULT_WRITE_MODE)
        with self._writer(key, **kwargs) as fh:
            fh.write(value)

    def _read(
        self, key: str, raise_on_nonexist: bool | None = True, **kwargs
    ) -> Value | None:
        kwargs["mode"] = kwargs.pop("mode", DEFAULT_MODE)
        try:
            self._check_delete(key)
            with self._reader(key, **kwargs) as fh:
                return fh.read()
        except (KeyError, DoesNotExist):
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    def _stream(
        self, key: str, raise_on_nonexist: bool | None = True, **kwargs
    ) -> ValueStream:
        kwargs["mode"] = kwargs.pop("mode", DEFAULT_MODE)
        try:
            self._check_delete(key)
            with self._reader(key, **kwargs) as fh:
                yield from fh
        except (KeyError, DoesNotExist):
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    def _exists(self, key: str) -> bool:
        with self._get_handler("r") as reader:
            if reader.exists(f".anystore/__DELETED__/{key}"):
                return False
            try:
                return reader.exists(key)
            except DoesNotExist:
                return False

    def _info(self, key: str) -> BaseStats:
        self._check_delete(key)
        with self._get_handler("r") as reader:
            return BaseStats(**reader.info(key))

    def _get_key_prefix(self) -> str:
        return ""

    def _delete(self, key: str) -> None:
        self._write(key, b"")
        # mark as deleted
        self._write(f".anystore/__DELETED__/{key}", b"")

    def _check_delete(self, key: str, do_raise: bool | None = True) -> bool:
        if self._exists(f".anystore/__DELETED__/{key}"):
            if do_raise:
                raise FileNotFoundError(key)
            return True
        return False

    def _bytes_io(self, key: str, **kwargs) -> BinaryIO:
        kwargs["mode"] = kwargs.pop("mode", DEFAULT_MODE)
        if "r" in kwargs["mode"]:
            self._check_delete(key)
            return self._reader(key, **kwargs)
        return self._writer(key, **kwargs)

    def _iterate_keys(self, prefix: str | None = None) -> Generator[str, None, None]:
        with self._get_handler("r") as reader:
            for member in reader.ls(prefix or ""):
                if not member["name"].startswith(".anystore/__DELETED__/"):
                    if not self._check_delete(member["name"], False):
                        if member["type"] == "directory":
                            yield from self._iterate_keys(member["name"])
                        else:
                            yield member["name"]
