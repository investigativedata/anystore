from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Callable, Generator
from urllib.parse import urljoin, urlparse

from pydantic import field_validator

from anystore.exceptions import DoesNotExist, WriteError
from anystore.io import DEFAULT_MODE
from anystore.serialize import Mode, from_store, to_store
from anystore.settings import Settings
from anystore.model import StoreModel, BaseStats, Stats
from anystore.types import Uri, Value, Model
from anystore.util import clean_dict, ensure_uri, make_checksum


settings = Settings()


def check_readonly(func: Callable):
    def _check(store: "BaseStore", *args, **kwargs):
        if store.readonly:
            raise WriteError(f"Store `{store.uri}` is configured readonly!")
        return func(store, *args, **kwargs)

    return _check


class BaseStore(StoreModel):
    def __init__(self, **data):
        uri = data.get("uri") or settings.uri
        data["scheme"] = urlparse(str(uri)).scheme
        super().__init__(**data)

    def _write(self, key: str, value: Value, **kwargs) -> None:
        """
        Write value with key to acutal backend
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

    def _stream(self, key: str, raise_on_nonexist: bool | None = True, **kwargs) -> Any:
        """
        Stream key line by line from actual backend (for file-like powered backend)
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def _iterate_keys(self, prefix: str | None = None) -> Generator[str, None, None]:
        """
        Backend specific key iterator
        """
        raise NotImplementedError

    def _bytes_io(self, key: str, **kwargs) -> BinaryIO:
        """
        Get a bytes io handler
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
    def delete(self, key: Uri) -> None:
        self._delete(self.get_key(key))

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
        deserialization_func = deserialization_func or self.deserialization_func
        model = model or self.model
        try:
            for line in self._stream(key, raise_on_nonexist, **kwargs):
                yield from_store(
                    line,
                    serialization_mode,
                    deserialization_func=deserialization_func,
                    model=model,
                )
        except FileNotFoundError:  # fsspec
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
        return Stats(
            **stats.model_dump(),
            name=Path(str(key)).name,
            store=str(self.uri),
            path=self.get_key(key),
            key=str(key),
        )

    def ensure_kwargs(self, **kwargs) -> dict[str, Any]:
        config = clean_dict(self.backend_config)
        return {**config, **clean_dict(kwargs)}

    def get_key(self, key: Uri) -> str:
        if self.prefix:
            return f"{self._get_key_prefix()}/{self.prefix}/{str(key)}".strip("/")
        return f"{self._get_key_prefix()}/{str(key)}".strip("/")

    def iterate_keys(self, prefix: str | None = None) -> Generator[str, None, None]:
        yield from self._iterate_keys(prefix)

    def checksum(self, key: Uri, algorithm: str | None = "md5", **kwargs) -> str:
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        with self._bytes_io(key, **kwargs) as io:
            return make_checksum(io, algorithm)

    def open(self, key: Uri, **kwargs) -> Any:
        mode = kwargs.get("mode", DEFAULT_MODE)
        if self.readonly and ("w" in mode or "a" in mode):
            raise WriteError(f"Store `{self.uri}` is configured readonly!")
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        return self._bytes_io(key, **kwargs)

    @check_readonly
    def touch(self, key: Uri, **kwargs) -> None:
        now = datetime.now()
        self.put(key, now, **kwargs)

    def __truediv__(self, prefix: Uri) -> "BaseStore":
        """
        Returns a new store, like:
            store = Store(uri="foo")
            new_store = store / "bar"
            assert new_store.uri == "foo/bar"
        """
        prefix = str(prefix).lstrip("/")
        if self.prefix:
            prefix = urljoin(self.prefix + "/", prefix)
        if prefix.startswith(".."):
            raise ValueError(f"Invalid path: `{prefix}`")
        return self.__class__(**{**self.model_dump(), "prefix": prefix})

    @field_validator("uri", mode="before")
    @classmethod
    def ensure_uri(cls, v: Any) -> str:
        uri = ensure_uri(v)
        return uri.rstrip("/")

    @field_validator("prefix", mode="before")
    @classmethod
    def ensure_prefix(cls, v: Any) -> str:
        return str(v or "").rstrip("/")
