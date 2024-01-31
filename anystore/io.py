import contextlib
import logging
import sys
from typing import Any, BinaryIO, Generator, TextIO

from fsspec import open
from fsspec.core import OpenFile

from anystore.util import ensure_uri

log = logging.getLogger(__name__)

DEFAULT_MODE = "rb"


def _get_sysio(mode: str | None = DEFAULT_MODE) -> TextIO | BinaryIO:
    if mode and mode.startswith("r"):
        return sys.stdin
    return sys.stdout


class SmartHandler:
    def __init__(
        self,
        uri: Any,
        *args,
        **kwargs,
    ) -> None:
        self.uri = ensure_uri(uri)
        self.is_buffer = self.uri == "-"
        self.args = args
        kwargs["mode"] = kwargs.get("mode", DEFAULT_MODE)
        self.sys_io = _get_sysio(kwargs["mode"])
        if kwargs["mode"].endswith("b"):
            self.sys_io = self.sys_io.buffer
        self.kwargs = kwargs
        self.handler: OpenFile | TextIO | None = None

    def open(self):
        if self.is_buffer:
            self.handler = self.sys_io
        else:
            handler = open(self.uri, *self.args, **self.kwargs)
            self.handler = handler.open()
        return self.handler

    def close(self):
        if not self.is_buffer and self.handler is not None:
            self.handler.close()

    def __enter__(self):
        return self.open()

    def __exit__(self, *args, **kwargs) -> None:
        self.close()


@contextlib.contextmanager
def smart_open(
    uri: Any,
    mode: str | None = None,
    *args,
    **kwargs,
):
    if mode is not None:
        kwargs["mode"] = mode
    else:
        kwargs["mode"] = kwargs.get("mode", DEFAULT_MODE)
    handler = SmartHandler(uri, *args, **kwargs)
    try:
        yield handler.open()
    finally:
        handler.close()


def smart_stream(uri, *args, **kwargs) -> Generator[str | bytes, None, None]:
    with smart_open(uri, *args, **kwargs) as fh:
        while line := fh.readline():
            yield line


def smart_read(uri, *args, **kwargs) -> Any:
    with smart_open(uri, *args, **kwargs) as fh:
        return fh.read()


def smart_write(uri, content: bytes | str, *args, **kwargs) -> None:
    kwargs["mode"] = kwargs.get("mode", "wb")
    with smart_open(uri, *args, **kwargs) as fh:
        fh.write(content)
