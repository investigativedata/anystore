from os import PathLike
from pathlib import Path
from typing import Type, TypeAlias

from pydantic import BaseModel


Uri: TypeAlias = PathLike | Path | str
Value: TypeAlias = str | bytes
Model: TypeAlias = Type[BaseModel]
