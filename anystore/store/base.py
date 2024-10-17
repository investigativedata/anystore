import contextlib
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, BinaryIO, Callable, Generator, TextIO
from urllib.parse import unquote

from anystore.exceptions import DoesNotExist, ReadOnlyError
from anystore.io import DEFAULT_MODE
from anystore.model import BaseStats, Stats, StoreModel
from anystore.serialize import Mode, from_store, to_store
from anystore.settings import Settings
from anystore.types import BytesGenerator, Model, Uri, Value
from anystore.util import DEFAULT_HASH_ALGORITHM, clean_dict, make_checksum

settings = Settings()


def check_readonly(func: Callable):
    def _check(store: "BaseStore", *args, **kwargs):
        if store.readonly:
            raise ReadOnlyError(f"Store `{store.uri}` is configured readonly!")
        return func(store, *args, **kwargs)

    return _check


class BaseStore(StoreModel):
    def _write(self, key: str, value: Value, **kwargs) -> None:
        """
        Write value with key to actual backend
        """
        raise NotImplementedError

    def _read(self, key: str, raise_on_nonexist: bool | None = True, **kwargs) -> Any:
        """
        Read key from actual backend
        """
        raise NotImplementedError

    def _delete(self, key: str) -> None:
        """
        Delete key from actual backend
        """
        raise NotImplementedError

    def _stream(self, key: str, **kwargs) -> BytesGenerator:
        """
        Stream key line by line from actual backend
        """
        kwargs["mode"] = "rb"
        with self._open(key, **kwargs) as i:
            while line := i.readline():
                yield line

    def _exists(self, key: str) -> bool:
        """
        Check if the given key exists
        """
        raise NotImplementedError

    def _info(self, key: str) -> BaseStats:
        """
        Get metadata about key and its value
        """
        raise NotImplementedError

    def _get_key_prefix(self) -> str:
        """
        Get backend specific key prefix
        """
        return ""

    def _iterate_keys(
        self,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        glob: str | None = None,
    ) -> Generator[str, None, None]:
        """
        Backend specific key iterator
        """
        raise NotImplementedError

    def _get_relpath(self, key: str) -> str:
        """
        Get relative path to the given key (backend specific)
        """
        return self.get_key(key).replace(self.uri, "").strip("/")

    @contextlib.contextmanager
    def _open(self, key: str, **kwargs) -> BinaryIO | TextIO:
        """
        Get a io handler
        """
        raise NotImplementedError

    def get(
        self,
        key: Uri,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Any:
        serialization_mode = serialization_mode or self.serialization_mode
        deserialization_func = deserialization_func or self.deserialization_func
        model = model or self.model
        if raise_on_nonexist is None:
            raise_on_nonexist = self.raise_on_nonexist
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        try:
            return from_store(
                self._read(key, raise_on_nonexist, **kwargs),
                serialization_mode,
                deserialization_func=deserialization_func,
                model=model,
            )
        except FileNotFoundError:  # fsspec
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    @check_readonly
    def pop(self, key: Uri, *args, **kwargs) -> Any:
        value = self.get(key, *args, **kwargs)
        self._delete(self.get_key(key))
        return value

    @check_readonly
    def delete(self, key: Uri, ignore_errors: bool = False) -> None:
        try:
            self._delete(self.get_key(key))
        except Exception as e:
            if not ignore_errors:
                raise e

    def stream(
        self,
        key: Uri,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Generator[Any, None, None]:
        key = self.get_key(key)
        model = model or self.model
        extra_kwargs = {
            "serialization_mode": serialization_mode or self.serialization_mode,
            "deserialization_func": deserialization_func or self.deserialization_func,
            "model": model,
        }
        try:
            for line in self._stream(key, **kwargs):
                yield from_store(line, **extra_kwargs)
        except (FileNotFoundError, DoesNotExist):
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    @check_readonly
    def put(
        self,
        key: Uri,
        value: Any,
        serialization_mode: Mode | None = None,
        serialization_func: Callable | None = None,
        model: Model | None = None,
        ttl: int | None = None,
        **kwargs,
    ):
        serialization_mode = serialization_mode or self.serialization_mode
        serialization_func = serialization_func or self.serialization_func
        model = model or self.model
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        ttl = ttl or self.default_ttl or None
        self._write(
            key,
            to_store(
                value,
                serialization_mode,
                serialization_func=serialization_func,
                model=model,
            ),
            ttl=ttl,
        )

    def exists(self, key: Uri) -> bool:
        return self._exists(self.get_key(key))

    def info(self, key: Uri) -> Stats:
        stats = self._info(self.get_key(key))
        key = str(key)
        return Stats(
            **stats.model_dump(),
            name=Path(key).name,
            store=str(self.uri),
            key=key,
        )

    def ensure_kwargs(self, **kwargs) -> dict[str, Any]:
        config = clean_dict(self.backend_config)
        return {**config, **clean_dict(kwargs)}

    def get_key(self, key: Uri) -> str:
        return f"{self._get_key_prefix()}/{str(key)}".strip("/")

    def iterate_keys(
        self,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        glob: str | None = None,
    ) -> Generator[str, None, None]:
        for key in self._iterate_keys(prefix, exclude_prefix, glob):
            yield unquote(key)

    def checksum(
        self, key: Uri, algorithm: str | None = DEFAULT_HASH_ALGORITHM, **kwargs
    ) -> str:
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        with self._open(key, **kwargs) as io:
            return make_checksum(io, algorithm or DEFAULT_HASH_ALGORITHM)

    def open(self, key: Uri, mode: str | None = DEFAULT_MODE, **kwargs) -> Any:
        mode = mode or DEFAULT_MODE
        if self.readonly and ("w" in mode or "a" in mode):
            raise ReadOnlyError(f"Store `{self.uri}` is configured readonly!")
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        return self._open(key, mode=mode, **kwargs)

    @check_readonly
    def touch(self, key: Uri, **kwargs) -> None:
        now = datetime.now()
        self.put(key, now, **kwargs)


class VirtualIOMixin:
    @contextlib.contextmanager
    def _open(self, key: str, **kwargs) -> Generator[BytesIO | StringIO, None, None]:
        mode = kwargs.get("mode", DEFAULT_MODE)
        writer = "w" in mode
        if not writer:
            content = self._read(key, **kwargs)
            if "b" in mode:
                handler = BytesIO(content)
            else:
                handler = StringIO(content)
        else:
            if "b" in mode:
                handler = BytesIO()
            else:
                handler = StringIO()
        try:
            yield handler
        finally:
            if writer:
                self._write(key, handler.getvalue(), **kwargs)
            handler.close()
