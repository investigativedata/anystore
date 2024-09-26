import shutil
from anystore.io import smart_open, smart_stream
from anystore.types import Uri
from anystore.store.base import BaseStore
import shortuuid
import tempfile
from anystore.store import get_store


class VirtualStore:
    """
    Temporary file storage for local processing
    """

    def __init__(self) -> None:
        self.path = tempfile.mkdtemp(prefix="leakrfc-")
        self.store = get_store(uri=self.path, serialization_mode="raw")

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
            shutil.rmtree(self.path, ignore_errors=True)
        except Exception:
            pass


def get_virtual() -> VirtualStore:
    return VirtualStore()
