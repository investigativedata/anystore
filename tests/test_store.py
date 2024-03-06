import pytest
from moto import mock_aws
from anystore.exceptions import DoesNotExist

from anystore.store import Store, get_store
from anystore.store.base import BaseStore
from tests.conftest import setup_s3


def _test_store(uri: str) -> bool:
    # generic store test
    store = get_store(uri=uri)
    assert isinstance(store, BaseStore)
    key = "test"
    store.put(key, "foo")
    assert store.get(key) == b"foo"
    assert store.get(key, mode="r") == "foo"
    # overwrite
    store.put(key, False)
    assert store.get(key) is False
    store.put("other", None)
    assert store.get("other") is None
    store.put("foo/bar/baz", 1)
    assert store.get("foo/bar/baz") == 1
    # non existing key
    with pytest.raises(DoesNotExist):
        store.get("nothing")
    assert store.get("nothing", raise_on_nonexist=False) is None
    return True


@mock_aws
def test_store_s3():
    setup_s3()
    assert _test_store("s3://anystore")


def test_store_redis():
    assert _test_store("redis:///localhost")


def test_store_sql(tmp_path):
    assert _test_store(f"sqlite:///{tmp_path}/db.sqlite")


def test_store_fs(tmp_path, fixtures_path):
    assert _test_store(tmp_path)

    # don't pickle "external" data
    store = Store(uri=fixtures_path)
    content = store.get("lorem.txt", mode="r")
    assert content.startswith("Lorem")

    # put into not yet existing sub paths
    store = Store(uri=tmp_path / "foo", raise_on_nonexist=False)
    store.put("/bar/baz", 1)
    assert (tmp_path / "foo/bar/baz").exists()
    assert store.get("/bar/baz") == 1


def test_store_intialize(fixtures_path):
    # initialize (take env vars into account)
    store = get_store()
    assert store.uri == "s3://anystore/another-store"
    assert get_store(uri="foo").uri.endswith("foo")

    store = Store.from_json_uri(fixtures_path / "store.json")
    assert store.uri == "file:///tmp/cache"

    store = Store(uri="s3://anystore", raise_on_nonexist=False)
