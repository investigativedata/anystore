from os import PathLike
import contextlib
from pathlib import Path
import sys
from typing import Any, BinaryIO, Generator, TextIO, TypeAlias

from fsspec import open
from fsspec.core import OpenFile

from anystore.exceptions import DoesNotExist
from anystore.logging import get_logger
from anystore.util import ensure_uri

log = get_logger(__name__)

DEFAULT_MODE = "rb"
DEFAULT_WRITE_MODE = "wb"

Uri: TypeAlias = PathLike | Path | BinaryIO | TextIO | str


def _get_sysio(mode: str | None = DEFAULT_MODE) -> TextIO | BinaryIO:
    if mode and mode.startswith("r"):
        return sys.stdin
    return sys.stdout


class SmartHandler:
    def __init__(
        self,
        uri: Uri,
        **kwargs,
    ) -> None:
        self.uri = ensure_uri(uri)
        self.is_buffer = self.uri == "-"
        kwargs["mode"] = kwargs.get("mode", DEFAULT_MODE)
        self.sys_io = _get_sysio(kwargs["mode"])
        if hasattr(self.sys_io, "buffer"):
            self.sys_io = self.sys_io.buffer
        self.kwargs = kwargs
        self.handler: OpenFile | TextIO | None = None

    def open(self):
        try:
            if self.is_buffer:
                self.handler = self.sys_io
            else:
                handler = open(self.uri, **self.kwargs)
                self.handler = handler.open()
            return self.handler
        except FileNotFoundError as e:
            raise DoesNotExist from e

    def close(self):
        if not self.is_buffer and self.handler is not None:
            self.handler.close()

    def __enter__(self):
        return self.open()

    def __exit__(self, *args, **kwargs) -> None:
        self.close()


@contextlib.contextmanager
def smart_open(
    uri: Uri,
    mode: str | None = DEFAULT_MODE,
    **kwargs,
):
    handler = SmartHandler(uri, mode=mode, **kwargs)
    try:
        yield handler.open()
    except FileNotFoundError as e:
        raise DoesNotExist from e
    finally:
        handler.close()


def smart_stream(
    uri: Uri, mode: str | None = DEFAULT_MODE, **kwargs
) -> Generator[str | bytes, None, None]:
    with smart_open(uri, mode, **kwargs) as fh:
        while line := fh.readline():
            yield line


def smart_read(uri: Uri, mode: str | None = DEFAULT_MODE, **kwargs) -> Any:
    with smart_open(uri, mode, **kwargs) as fh:
        return fh.read()


def smart_write(
    uri: Uri, content: bytes | str, mode: str | None = DEFAULT_WRITE_MODE, **kwargs
) -> None:
    if uri == "-":
        if isinstance(content, str):
            content = content.encode()
    with smart_open(uri, mode, **kwargs) as fh:
        fh.write(content)
