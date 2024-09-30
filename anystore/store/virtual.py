import shutil
import tempfile

import shortuuid

from anystore.store import get_store, get_store_for_uri
from anystore.store.base import BaseStore
from anystore.types import Uri


class VirtualStore:
    """
    Temporary file storage for local processing
    """

    def __init__(self, prefix: str | None = None) -> None:
        self.path = tempfile.mkdtemp(prefix=(prefix or "anystore") + "-")
        self.store = get_store(uri=self.path, serialization_mode="raw")

    def download(self, uri: Uri, store: BaseStore | None = None) -> str:
        key = shortuuid.uuid()
        if store is None:
            store, uri = get_store_for_uri(uri, serialization_mode="raw")
        with store.open(uri, mode="rb") as i:
            with self.store.open(key, mode="wb") as o:
                o.write(i.read())
        return key

    def cleanup(self) -> None:
        try:
            shutil.rmtree(self.path, ignore_errors=True)
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.cleanup()


def get_virtual() -> VirtualStore:
    return VirtualStore()
