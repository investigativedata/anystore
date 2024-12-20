import contextlib
import tempfile
from pathlib import Path
from typing import BinaryIO, Generator

from anystore.store import get_store, get_store_for_uri
from anystore.store.base import BaseStore
from anystore.types import Uri
from anystore.util import rm_rf


class VirtualStore:
    """
    Temporary file storage for local processing
    """

    def __init__(self, prefix: str | None = None, keep: bool | None = False) -> None:
        self.path = tempfile.mkdtemp(prefix=(prefix or "anystore-"))
        self.store = get_store(uri=self.path, serialization_mode="raw")
        self.keep = keep

    def download(self, uri: Uri, store: BaseStore | None = None) -> str:
        if store is None:
            store, uri = get_store_for_uri(uri, serialization_mode="raw")
        with store.open(uri, mode="rb") as i:
            with self.store.open(uri, mode="wb") as o:
                o.write(i.read())
        return str(uri)

    def cleanup(self, path: Uri | None = None) -> None:
        if path is not None:
            path = Path(self.path) / path
            rm_rf(path)
        else:
            rm_rf(self.path)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if not self.keep:
            self.cleanup()


def get_virtual(prefix: str | None = None, keep: bool | None = False) -> VirtualStore:
    return VirtualStore(prefix, keep=keep)


@contextlib.contextmanager
def open_virtual(
    uri: Uri,
    store: BaseStore | None = None,
    tmp_prefix: str | None = None,
    keep: bool | None = False,
) -> Generator[BinaryIO, None, None]:
    tmp = VirtualStore(tmp_prefix, keep)
    key = tmp.download(uri, store)
    try:
        with tmp.store.open(key) as handler:
            yield handler
    finally:
        if not keep:
            tmp.cleanup()
