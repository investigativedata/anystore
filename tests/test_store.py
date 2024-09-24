import time
from moto import mock_aws
import pytest

from anystore.exceptions import DoesNotExist
from anystore.io import smart_read
from anystore.store import Store, get_store
from anystore.store.base import BaseStore
from anystore.store.memory import MemoryStore
from anystore.store.redis import RedisStore
from anystore.store.sql import SqlStore
from anystore.store.virtual import get_virtual
from tests.conftest import setup_s3


def _test_store(fixtures_path, uri: str) -> bool:
    # generic store test
    store = get_store(uri=uri)
    assert isinstance(store, BaseStore)
    key = "test"
    store.put(key, "foo")
    assert store.get(key) == "foo"
    assert store.get(key, mode="r") == "foo"

    store.put("seri", "HELLO", serialization_func=lambda x: x.lower().encode())
    assert store.get("seri") == "hello"
    assert (
        store.pop("seri", deserialization_func=lambda x: x.decode().upper()) == "HELLO"
    )

    # overwrite
    store.put(key, False)
    assert store.get(key) is False
    store.put("other", None)
    assert store.get("other") is None
    store.put("foo/bar/baz", 1)
    assert store.get("foo/bar/baz") == 1
    assert store.exists("foo/bar/baz") is True
    # non existing key
    with pytest.raises(DoesNotExist):
        store.get("nothing")
    assert store.get("nothing", raise_on_nonexist=False) is None
    assert store.exists("nothing") is False

    # iterate
    keys = [k for k in store.iterate_keys()]
    assert len(keys) == 3
    assert all(store.exists(k) for k in keys)
    keys = [k for k in store.iterate_keys("foo")]
    assert keys[0] == "foo/bar/baz"
    assert len(keys) == 1
    keys = [k for k in store.iterate_keys("foo/bar")]
    assert len(keys) == 1
    assert keys[0] == "foo/bar/baz"

    # pop
    store.put("popped", 1)
    assert store.pop("popped") == 1
    assert store.get("popped", raise_on_nonexist=False) is None

    # ttl
    if isinstance(store, (RedisStore, SqlStore, MemoryStore)):
        store.put("expired", 1, ttl=1)
        assert store.get("expired") == 1
        time.sleep(1)
        assert store.get("expired", raise_on_nonexist=False) is None

    # checksum
    md5sum = "6d484beb4162b026abc7cfea019acbd1"
    sha1sum = "ed3141878ed32d8a1d583e7ce7de323118b933d3"
    lorem = smart_read(fixtures_path / "lorem.txt")
    store.put("lorem", lorem)
    assert store.checksum("lorem") == md5sum
    assert store.checksum("lorem", "sha1") == sha1sum

    return True


@mock_aws
def test_store_s3(fixtures_path):
    setup_s3()
    assert _test_store(fixtures_path, "s3://anystore")


def test_store_redis(fixtures_path):
    assert _test_store(fixtures_path, "redis:///localhost")


def test_store_sql(fixtures_path, tmp_path):
    assert _test_store(fixtures_path, f"sqlite:///{tmp_path}/db.sqlite")


def test_store_memory(fixtures_path):
    assert _test_store(fixtures_path, "memory:///")


def test_store_fs(tmp_path, fixtures_path):
    assert _test_store(fixtures_path, tmp_path)

    # don't pickle "external" data
    store = Store(uri=fixtures_path)
    content = store.get("lorem.txt", mode="r")
    assert content.startswith("Lorem")

    # put into not yet existing sub paths
    store = Store(uri=tmp_path / "foo", raise_on_nonexist=False)
    store.put("/bar/baz", 1)
    assert (tmp_path / "foo/bar/baz").exists()
    assert store.get("/bar/baz") == 1

    # stream
    store = Store(uri=fixtures_path)
    tested = False
    for ix, line in enumerate(store.stream("lorem.txt", mode="r")):
        if ix == 1:
            assert line.startswith("tempor")
            tested = True
            break
    assert tested


def test_store_intialize(fixtures_path):
    # initialize (take env vars into account)
    get_store.cache_clear()
    store = get_store()
    assert store.uri == "s3://anystore/another-store"
    assert get_store(uri="foo").uri.endswith("foo")

    store = Store.from_json_uri(fixtures_path / "store.json")
    assert store.uri == "file:///tmp/cache"

    store = Store(uri="s3://anystore", raise_on_nonexist=False)
    assert store.raise_on_nonexist is False


def test_store_virtual(fixtures_path):
    lorem = smart_read(fixtures_path / "lorem.txt")
    tmp = get_virtual()
    key = tmp.download(fixtures_path / "lorem.txt")
    assert tmp.store.pop(key) == lorem

    store = get_store(uri=fixtures_path)
    key = tmp.download("lorem.txt", store)
    assert tmp.store.pop(key) == lorem
