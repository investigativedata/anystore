import shutil
from anystore.io import smart_open, smart_stream
from anystore.types import Uri
from anystore.store.base import BaseStore
import shortuuid
import tempfile
from functools import cached_property
import threading
from anystore.store import get_store
from anystore.store.fs import Store


class VirtualStore:
    """
    Temporary file storage for local processing
    """

    def __init__(self) -> None:
        self.local = threading.local()

    @cached_property
    def store(self) -> Store:
        if not hasattr(self.local, "store"):
            self.local.dir = tempfile.mkdtemp(prefix="leakrfc-")
            self.local.store = get_store(uri=self.local.dir, serialization_mode="raw")
        return self.local.store

    def download(self, uri: Uri, store: BaseStore | None = None) -> str:
        key = shortuuid.uuid()
        if store is not None:
            lines = store.stream(uri, serialization_mode="raw")
        else:
            lines = smart_stream(uri)

        with smart_open(self.store.get_key(key), "wb") as fh:
            fh.writelines(lines)
        return key

    def cleanup(self) -> None:
        try:
            shutil.rmtree(self.local.dir, ignore_errors=True)
        except Exception:
            pass


def get_virtual() -> VirtualStore:
    return VirtualStore()
