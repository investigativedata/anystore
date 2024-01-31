from functools import cache
import logging
from typing import Self

import orjson
from pydantic import BaseModel as _BaseModel
from pydantic import field_validator
import yaml

from anystore.io import smart_read
from anystore.types import Uri
from anystore.util import clean_dict

log = logging.getLogger(__name__)


@cache
def cached_from_uri(uri: Uri) -> str:
    """
    Cache remote sources on runtime
    """
    log.info("Loading `%s` ..." % uri)
    return smart_read(uri)


class JsonMixin:
    """
    Load a pydantic model from a local or remote uri with json data
    """

    @classmethod
    def from_json_str(cls, data: str) -> Self:
        return cls(**clean_dict(orjson.loads(data)))

    @classmethod
    def from_json_uri(cls, uri: Uri) -> Self:
        data = cached_from_uri(uri)
        return cls.from_json_str(data)


class YamlMixin:
    """
    Load a pydantic model from a local or remote uri with yaml data
    """

    @classmethod
    def from_yaml_str(cls, data: str) -> "YamlMixin":
        return cls(**yaml.safe_load(data))

    @classmethod
    def from_yaml_uri(cls, uri: Uri) -> "YamlMixin":
        data = cached_from_uri(uri)
        return cls.from_yaml_str(data)


class BaseModel(_BaseModel, JsonMixin, YamlMixin):
    def __hash__(self) -> int:
        return hash(repr(self.model_dump()))

    @field_validator("*", mode="before")
    @classmethod
    def empty_str_to_none(cls, v) -> str | None:
        if v == "":
            return None
        return v
