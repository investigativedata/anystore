from datetime import datetime
from functools import cached_property
from typing import Any, Callable
from urllib.parse import urlparse

from pydantic import field_validator

from anystore.mixins import BaseModel
from anystore.serialize import Mode
from anystore.settings import Settings
from anystore.types import Model
from anystore.util import ensure_uri, join_uri

settings = Settings()

SCHEME_FILE = "file"
SCHEME_S3 = "s3"
SCHEME_REDIS = "redis"
SCHEME_MEMORY = "memory"


class BaseStats(BaseModel):
    created_at: datetime | None = None
    updated_at: datetime | None = None
    size: int


class Stats(BaseStats):
    name: str
    store: str
    key: str

    @property
    def uri(self) -> str:
        store = StoreModel(uri=self.store)
        if store.is_fslike:
            return join_uri(self.store, self.key)
        return self.key


class StoreModel(BaseModel):
    uri: str
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
    def path(self) -> str:
        return urlparse(self.uri).path.strip("/")

    @cached_property
    def netloc(self) -> str:
        return urlparse(self.uri).netloc

    @cached_property
    def is_local(self) -> bool:
        return self.scheme == SCHEME_FILE

    @cached_property
    def is_fslike(self) -> bool:
        return not self.is_sql and self.scheme not in (SCHEME_REDIS, SCHEME_MEMORY)

    @cached_property
    def is_sql(self) -> bool:
        return "sql" in self.scheme

    @field_validator("uri", mode="before")
    @classmethod
    def ensure_uri(cls, v: Any) -> str:
        uri = ensure_uri(v)
        return uri.rstrip("/")
