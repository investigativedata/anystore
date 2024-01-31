from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from banal import clean_dict as _clean_dict
from banal import is_mapping


def clean_dict(data: Any) -> dict[str, Any]:
    """
    strip out defaultdict and ensure str keys (for serialization)
    """
    if not is_mapping(data):
        return {}
    return _clean_dict(
        {
            str(k): clean_dict(dict(v)) or None if is_mapping(v) else v or None
            for k, v in data.items()
        }
    )


def ensure_uri(uri: Any) -> str:
    if not uri:
        raise ValueError(f"Invalid uri: `{uri}`")
    if uri == "-":  # stdin/stoud
        return uri
    if isinstance(uri, Path):
        return uri.absolute().as_uri()
    if isinstance(uri, str) and not uri.strip():
        raise ValueError(f"Invalid uri: `{uri}`")
    uri = str(uri)
    parsed = urlparse(uri)
    if parsed.scheme:
        return uri
    return Path(uri).absolute().as_uri()
