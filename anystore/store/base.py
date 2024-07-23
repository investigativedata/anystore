from typing import Any, Generator
from urllib.parse import urlparse

from pydantic import field_validator

from anystore.exceptions import DoesNotExist
from anystore.mixins import BaseModel
from anystore.serialize import Mode, from_store, to_store
from anystore.settings import Settings
from anystore.types import Uri, Value, Model
from anystore.util import clean_dict, ensure_uri


settings = Settings()


class BaseStore(BaseModel):
    uri: Uri | None = settings.uri
    scheme: str | None = None
    serialization_mode: Mode | None = settings.serialization_mode
    model: Model | None = None
    raise_on_nonexist: bool | None = settings.raise_on_nonexist
    backend_config: dict[str, Any] | None = None

    def __init__(self, **data):
        data["scheme"] = urlparse(str(data["uri"])).scheme
        super().__init__(**data)

    def _write(self, key: Uri, value: Value, **kwargs) -> None:
        """
        Write value with key to acutal backend
        """
        raise NotImplementedError

    def _read(self, key: Uri, raise_on_nonexist: bool | None = True, **kwargs) -> Any:
        """
        Read key from actual backend
        """
        raise NotImplementedError

    def _stream(self, key: Uri, raise_on_nonexist: bool | None = True, **kwargs) -> Any:
        """
        Stream key line by line from actual backend (for file-like powered backend)
        """
        raise NotImplementedError

    def _exists(self, key: Uri) -> bool:
        """
        Check if the given key exists
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

    def get(
        self,
        key: Uri,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Any:
        serialization_mode = serialization_mode or self.serialization_mode
        model = model or self.model
        if raise_on_nonexist is None:
            raise_on_nonexist = self.raise_on_nonexist
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        try:
            return from_store(
                self._read(key, raise_on_nonexist, **kwargs),
                serialization_mode,
                model=model,
            )
        except FileNotFoundError:  # fsspec
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    def stream(
        self,
        key: Uri,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Generator[Any, None, None]:
        key = self.get_key(key)
        model = model or self.model
        try:
            for line in self._stream(key, raise_on_nonexist, **kwargs):
                yield from_store(line, serialization_mode, model=model)
        except FileNotFoundError:  # fsspec
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    def put(
        self,
        key: Uri,
        value: Any,
        serialization_mode: Mode | None = None,
        model: Model | None = None,
        **kwargs,
    ):
        serialization_mode = serialization_mode or self.serialization_mode
        model = model or self.model
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        self._write(key, to_store(value, serialization_mode, model=model))

    def exists(self, key: Uri) -> bool:
        return self._exists(key)

    def ensure_kwargs(self, **kwargs) -> dict[str, Any]:
        config = clean_dict(self.backend_config)
        return {**config, **clean_dict(kwargs)}

    def get_key(self, key: Uri) -> str:
        return f"{self._get_key_prefix()}/{str(key)}".strip("/")

    def iterate_keys(self, prefix: str | None = None) -> Generator[str, None, None]:
        yield from self._iterate_keys(prefix)

    @field_validator("uri", mode="before")
    @classmethod
    def ensure_uri(cls, v: Any) -> str:
        uri = ensure_uri(v)
        return uri.rstrip("/")
