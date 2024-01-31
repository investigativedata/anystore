from typing import Any
from urllib.parse import urljoin

from pydantic import field_validator
from anystore.exceptions import DoesNotExist
from anystore.io import smart_read, smart_write

from anystore.mixins import BaseModel
from anystore.serialize import from_cloud, to_cloud
from anystore.types import Uri
from anystore.util import clean_dict, ensure_uri


class Store(BaseModel):
    uri: str
    use_pickle: bool | None = True
    raise_on_nonexist: bool | None = True
    backend_config: dict[str, Any] | None = None

    def get(
        self,
        key: Uri,
        raise_on_nonexist: bool | None = None,
        use_pickle: bool | None = None,
        **kwargs,
    ) -> Any:
        if use_pickle is None:
            use_pickle = self.use_pickle
        if raise_on_nonexist is None:
            raise_on_nonexist = self.raise_on_nonexist
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        try:
            return from_cloud(smart_read(key, **kwargs), use_pickle)
        except FileNotFoundError:  # fsspec
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    def set(self, key: Uri, value: Any, use_pickle: bool | None = None, **kwargs):
        use_pickle = use_pickle or self.use_pickle
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        smart_write(key, to_cloud(value, use_pickle))

    def ensure_kwargs(self, **kwargs) -> dict[str, Any]:
        config = clean_dict(self.backend_config)
        return {**config, **clean_dict(kwargs)}

    def get_key(self, key: Uri) -> str:
        return urljoin(self.uri + "/", str(key))

    @field_validator("uri", mode="before")
    @classmethod
    def ensure_uri(cls, v: Any) -> str:
        return ensure_uri(v)