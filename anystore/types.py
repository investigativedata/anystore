from os import PathLike
from pathlib import Path
from typing import Any, AnyStr, Generator, Type, TypeAlias

from pydantic import BaseModel

Uri: TypeAlias = PathLike | Path | str
Value: TypeAlias = str | bytes
Model: TypeAlias = Type[BaseModel]
SDict: TypeAlias = dict[str, Any]

StrGenerator: TypeAlias = Generator[str, None, None]
BytesGenerator: TypeAlias = Generator[bytes, None, None]
AnyStrGenerator: TypeAlias = Generator[AnyStr, None, None]
SDictGenerator: TypeAlias = Generator[SDict, None, None]
