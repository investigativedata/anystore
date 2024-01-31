from os import PathLike
from pathlib import Path
from typing import TypeAlias


Uri: TypeAlias = PathLike | Path | str
