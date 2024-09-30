from typing import Any, Callable, Optional
from datetime import datetime

from anystore.mixins import BaseModel
from anystore.settings import Settings
from anystore.serialize import Mode
from anystore.util import join_uri
from anystore.types import Uri, Model

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
    uri: Uri | None = settings.uri
    prefix: Optional[str] = ""
    scheme: str | None = None
    serialization_mode: Mode | None = settings.serialization_mode
    serialization_func: Callable | None = None
    deserialization_func: Callable | None = None
    model: Model | None = None
    raise_on_nonexist: bool | None = settings.raise_on_nonexist
    default_ttl: int | None = settings.default_ttl
    backend_config: dict[str, Any] | None = None
    readonly: bool | None = False
