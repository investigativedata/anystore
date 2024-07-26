from anystore.store import get_store, Store
from anystore.decorators import anycache, async_anycache
from anystore.io import smart_read, smart_write, smart_stream

__all__ = [
    "get_store",
    "Store",
    "anycache",
    "async_anycache",
    "smart_read",
    "smart_write",
    "smart_stream",
]


__version__ = "0.1.8"
