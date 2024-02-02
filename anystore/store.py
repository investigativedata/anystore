from functools import cache
from typing import Any

from pydantic import field_validator
from anystore.exceptions import DoesNotExist
from anystore.io import smart_read, smart_write

from anystore.mixins import BaseModel
from anystore.serialize import from_store, to_store, Mode
from anystore.types import Uri
from anystore.util import clean_dict, ensure_uri
from anystore.settings import Settings


settings = Settings()


class Store(BaseModel):
    uri: str | None = settings.uri
    serialization_mode: Mode | None = settings.serialization_mode
    raise_on_nonexist: bool | None = settings.raise_on_nonexist
    backend_config: dict[str, Any] | None = None

    def get(
        self,
        key: Uri,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        **kwargs,
    ) -> Any:
        serialization_mode = serialization_mode or self.serialization_mode
        if raise_on_nonexist is None:
            raise_on_nonexist = self.raise_on_nonexist
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        try:
            return from_store(smart_read(key, **kwargs), serialization_mode)
        except FileNotFoundError:  # fsspec
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    def put(
        self, key: Uri, value: Any, serialization_mode: Mode | None = None, **kwargs
    ):
        serialization_mode = serialization_mode or self.serialization_mode
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        smart_write(key, to_store(value, serialization_mode))

    def ensure_kwargs(self, **kwargs) -> dict[str, Any]:
        config = clean_dict(self.backend_config)
        return {**config, **clean_dict(kwargs)}

    def get_key(self, key: Uri) -> str:
        return f"{self.uri}/{str(key).lstrip('/')}"

    @field_validator("uri", mode="before")
    @classmethod
    def ensure_uri(cls, v: Any) -> str:
        uri = ensure_uri(v)
        return uri.rstrip("/")


@cache
def get_store(**kwargs) -> Store:
    if "uri" not in kwargs:
        if settings.yaml_uri is not None:
            return Store.from_yaml_uri(settings.yaml_uri, **kwargs)
        if settings.json_uri is not None:
            return Store.from_json_uri(settings.json_uri, **kwargs)
    return Store(**kwargs)
