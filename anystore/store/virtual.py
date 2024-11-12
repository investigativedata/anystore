import contextlib
import shutil
import tempfile
from pathlib import Path
from typing import BinaryIO, Generator

import shortuuid

from anystore.store import get_store, get_store_for_uri
from anystore.store.base import BaseStore
from anystore.types import Uri
from anystore.util import get_extension


class VirtualStore:
    """
    Temporary file storage for local processing
    """

    def __init__(self, prefix: str | None = None) -> None:
        self.path = tempfile.mkdtemp(prefix=(prefix or "anystore-"))
        self.store = get_store(uri=self.path, serialization_mode="raw")

    def download(self, uri: Uri, store: BaseStore | None = None) -> str:
        ext = get_extension(uri) or ".lfc"
        key = f"{shortuuid.uuid()}.{ext}"
        if store is None:
            store, uri = get_store_for_uri(uri, serialization_mode="raw")
        with store.open(uri, mode="rb") as i:
            with self.store.open(key, mode="wb") as o:
                o.write(i.read())
        return key

    def cleanup(self, path: str | None = None) -> None:
        try:
            if path is not None:
                p = Path(self.path) / path
                if p.is_dir():
                    shutil.rmtree(str(p), ignore_errors=True)
                else:
                    p.unlink()
            else:
                shutil.rmtree(self.path, ignore_errors=True)
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.cleanup()


def get_virtual(prefix: str | None = None) -> VirtualStore:
    return VirtualStore(prefix)


@contextlib.contextmanager
def open_virtual(
    uri: Uri, store: BaseStore | None = None, tmp_prefix: str | None = None
) -> Generator[BinaryIO, None, None]:
    tmp = VirtualStore(tmp_prefix)
    key = tmp.download(uri, store)
    try:
        with tmp.store.open(key) as handler:
            yield handler
    finally:
        tmp.cleanup()
