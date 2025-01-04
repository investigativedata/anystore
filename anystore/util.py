import hashlib
import shutil
from io import BytesIO, StringIO
from os.path import splitext
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import unquote, urljoin, urlparse, urlsplit, urlunsplit

from banal import clean_dict as _clean_dict
from banal import is_mapping

from anystore.types import Uri

DEFAULT_HASH_ALGORITHM = "sha1"
SCHEME_FILE = "file"
SCHEME_S3 = "s3"
SCHEME_REDIS = "redis"
SCHEME_MEMORY = "memory"


def _clean(val: Any) -> Any:
    if val is False:
        return False
    return val or None


def clean_dict(data: Any) -> dict[str, Any]:
    """
    Ensure dict return, clean up defaultdicts, drop `None` values and ensure
    `str` keys (for serialization)

    Examples:
        >>> clean_dict({1: 2})
        {"1": 2}
        >>> clean_dict({"a": ""})
        {}
        >>> clean_dict({"a": None})
        {}
        >>> clean_dict("foo")
        {}

    Args:
        data: Arbitrary input data

    Returns:
        A cleaned dict with string keys (or an empty one)
    """
    if not is_mapping(data):
        return {}
    return _clean_dict(
        {
            str(k): clean_dict(dict(v)) or None if is_mapping(v) else _clean(v)
            for k, v in data.items()
        }
    )


def ensure_uri(uri: Any, http_unquote: bool | None = True) -> str:
    """
    Normalize arbitrary uri-like input to an absolute uri with scheme.

    Example:
        ```python
        assert util.ensure_uri("https://example.com") == "https://example.com"
        assert util.ensure_uri("s3://example.com") == "s3://example.com"
        assert util.ensure_uri("foo://example.com") == "foo://example.com"
        assert util.ensure_uri("-") == "-"
        assert util.ensure_uri("./foo").startswith("file:///")
        assert util.ensure_uri(Path("./foo")).startswith("file:///")
        assert util.ensure_uri("/foo") == "file:///foo"
        ```
    Args:
        uri: uri-like string
        http_unquote: Return unquoted uri, manually disable for some http edge cases

    Returns:
        Absolute uri with scheme

    Raises:
        ValueError: For invalid uri (e.g. stdin: "-")
    """
    if isinstance(uri, (BytesIO, StringIO)):
        raise ValueError(f"Invalid uri: `{uri}`")
    if not uri:
        raise ValueError(f"Invalid uri: `{uri}`")
    if uri == "-":  # stdin/stout
        return uri
    if isinstance(uri, Path):
        return unquote(uri.absolute().as_uri())
    if isinstance(uri, str) and not uri.strip():
        raise ValueError(f"Invalid uri: `{uri}`")
    uri = str(uri)
    parsed = urlparse(uri)
    if parsed.scheme:
        if parsed.scheme.startswith("http") and not http_unquote:
            return uri
        return unquote(uri)
    return unquote(Path(uri).absolute().as_uri())


def path_from_uri(uri: Uri) -> Path:
    """
    Get `pathlib.Path` object from a file uri

    Examples:
        >>> path_from_uri("/foo/bar")
        Path("/foo/bar")
        >>> path_from_uri("file:///foo/bar")
        Path("/foo/bar")

    Args:
        uri: (Full) path-like uri

    Returns:
        Path object for given uri
    """
    uri = ensure_uri(uri)
    if urlparse(uri).scheme != SCHEME_FILE:
        raise NotImplementedError(f"Wrong scheme: `{uri}`")
    return Path(uri[7:])  # file://


def name_from_uri(uri: Uri) -> str:
    """
    Extract the file name from an uri.

    Examples:
        >>> name_from_uri("/foo/bar.txt")
        bar.txt

    Args:
        uri: (Full) path-like uri

    Returns:
        File name
    """
    return path_from_uri(uri).name


def join_uri(uri: Any, path: str) -> str:
    """
    Ensure correct joining of arbitrary uris with a path.

    Example:
        ```python
        assert util.join_uri("http://example.org", "foo") == "http://example.org/foo"
        assert util.join_uri("http://example.org/", "foo") == "http://example.org/foo"
        assert util.join_uri("/tmp", "foo") == "file:///tmp/foo"
        assert util.join_uri(Path("./foo"), "bar").startswith("file:///")
        assert util.join_uri(Path("./foo"), "bar").endswith("foo/bar")
        assert util.join_uri("s3://foo/bar.pdf", "../baz.txt") == "s3://foo/baz.txt"
        assert util.join_uri("redis://foo/bar.pdf", "../baz.txt") == "redis://foo/baz.txt"
        ```

    Args:
        uri: Base uri
        path: Relative path to join on

    Returns:
        Absolute joined uri

    Raises:
        ValueError: For invalid uri (e.g. stdin: "-")
    """
    # FIXME wtf
    uri = ensure_uri(uri)
    if not uri or uri == "-":
        raise ValueError(f"Invalid uri: `{uri}`")
    uri += "/"
    scheme, *parts = urlsplit(uri)
    _, *parts = urlsplit(urljoin(urlunsplit(["", *parts]), path))
    return urlunsplit([scheme, *parts])


def join_relpaths(*parts: str) -> str:
    """
    Join relative paths, strip leading and trailing "/"

    Examples:
        >>> join_relpaths("/a/b/c/", "d/e")
        "a/b/c/d/e"

    Args:
        *parts: Relative path segments

    Returns:
        Joined relative path
    """
    return "/".join((p.strip("/") for p in parts if p)).strip("/")


def make_checksum(io: BinaryIO, algorithm: str = DEFAULT_HASH_ALGORITHM) -> str:
    """
    Calculate checksum for bytes input for given algorithm

    Example:
        This can be used for file handlers:

        ```python
        with open("data.pdf") as fh:
            return make_checksum(fh, algorithm="md5")
        ```

    Note:
        See [`make_data_checksum`][anystore.util.make_data_checksum] for easier
        implementation for arbitrary input data.

    Args:
        io: File-like open handler
        algorithm: Algorithm from `hashlib` to use, default: sha1

    Returns:
        Generated checksum
    """
    hash_ = getattr(hashlib, algorithm)()
    for chunk in iter(lambda: io.read(65536 * 128 * hash_.block_size), b""):
        hash_.update(chunk)
    return hash_.hexdigest()


def make_data_checksum(data: Any, algorithm: str = DEFAULT_HASH_ALGORITHM) -> str:
    """
    Calculate checksum for input data based on given algorithm

    Examples:
        >>> make_data_checksum({"foo": "bar"})
        "8f3536a88e3405de70ca2524cfd962203db9a84a"

    Args:
        data: Arbitrary input object
        algorithm: Algorithm from `hashlib` to use, default: sha1

    Returns:
        Generated checksum
    """
    if isinstance(data, bytes):
        return make_checksum(BytesIO(data), algorithm)
    if isinstance(data, str):
        return make_checksum(BytesIO(data.encode()), algorithm)
    data = repr(data).encode()
    return make_checksum(BytesIO(data), algorithm)


def make_signature_key(*args: Any, **kwargs: Any) -> str:
    """
    Calculate data checksum for arbitrary input (used for caching function
    calls)

    Examples:
        >>> make_signature_key(1, "foo", bar="baz")
        "c6b22da6bcf4bf7158ba600594cae404648acd41"

    Args:
        *args: Arbitrary input arguments
        **kwargs: Arbitrary input keyword arguments

    Returns:
        Generated sha1 checksum
    """
    return make_data_checksum((args, kwargs))


def get_extension(uri: Uri) -> str | None:
    """
    Extract file extension from given uri.

    Examples:
        >>> get_extension("foo/bar.txt")
        "txt"
        >>> get_extension("foo/bar")
        None

    Args:
        uri: Full path-like uri

    Returns:
        Extension or `None`
    """
    if isinstance(uri, (BytesIO, StringIO)):
        return None
    _, ext = splitext(str(uri))
    if ext:
        return ext[1:].lower()


def rm_rf(uri: Uri) -> None:
    """
    like rm -rf, ignoring errors.
    """
    try:
        p = Path(uri)
        if p.is_dir():
            shutil.rmtree(str(p), ignore_errors=True)
        else:
            p.unlink()
    except Exception:
        pass
