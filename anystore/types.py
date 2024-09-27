from os import PathLike
from pathlib import Path
from typing import Generator, Type, TypeAlias

from pydantic import BaseModel


Uri: TypeAlias = PathLike | Path | str
Value: TypeAlias = str | bytes
Model: TypeAlias = Type[BaseModel]

StrGenerator: TypeAlias = Generator[str, None, None]
BytesGenerator: TypeAlias = Generator[bytes, None, None]
ValueStream: TypeAlias = Generator[Value, None, None]
