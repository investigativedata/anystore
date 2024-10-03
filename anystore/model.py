from typing import Any, Callable, Optional
from datetime import datetime
from functools import cached_property
from urllib.parse import urlparse

from pydantic import field_validator

from anystore.mixins import BaseModel
from anystore.settings import Settings
from anystore.serialize import Mode
from anystore.util import join_uri, ensure_uri
from anystore.types import Model

settings = Settings()


class BaseStats(BaseModel):
    created_at: datetime | None = None
    updated_at: datetime | None = None
    size: int | None = None


class Stats(BaseStats):
    name: str
    store: str
    path: str
    key: str

    @property
    def uri(self) -> str:
        if self.store.startswith("file"):
            return self.path
        if self.store.startswith("http"):
            return self.path
        return join_uri(self.store, self.path)


class StoreModel(BaseModel):
    uri: str
    prefix: Optional[str] = ""
    serialization_mode: Mode | None = settings.serialization_mode
    serialization_func: Callable | None = None
    deserialization_func: Callable | None = None
    model: Model | None = None
    raise_on_nonexist: bool | None = settings.raise_on_nonexist
    default_ttl: int | None = settings.default_ttl
    backend_config: dict[str, Any] | None = None
    readonly: bool | None = False

    @cached_property
    def scheme(self) -> str:
        return urlparse(self.uri).scheme

    @cached_property
    def is_local(self) -> bool:
        return self.scheme == "file"

    @field_validator("uri", mode="before")
    @classmethod
    def ensure_uri(cls, v: Any) -> str:
        uri = ensure_uri(v)
        return uri.rstrip("/")

    @field_validator("prefix", mode="before")
    @classmethod
    def ensure_prefix(cls, v: Any) -> str:
        return str(v or "").rstrip("/")
