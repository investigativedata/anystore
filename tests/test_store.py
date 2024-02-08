from pathlib import Path
import pytest
from moto import mock_aws
from anystore.exceptions import DoesNotExist

from anystore.store import Store, get_store
from tests.conftest import setup_s3


@mock_aws
def test_store(tmp_path, fixtures_path):
    setup_s3()

    # store and retrieve
    store = Store(uri=tmp_path)
    key = "test"
    store.put(key, "foo")
    assert store.get(key) == b"foo"
    assert store.get(key, mode="r") == "foo"
    store.put(key, False)
    assert store.get(key) is False

    store = Store(uri="s3://anystore")
    key = "test"
    store.put(key, "foo")
    assert store.get(key) == b"foo"
    store.put(key, False)
    assert store.get(key) is False

    # don't pickle "external" data
    store = Store(uri=fixtures_path)
    content = store.get("lorem.txt", mode="r")
    assert content.startswith("Lorem")

    # non existing key
    store = Store(uri="s3://anystore")
    with pytest.raises(DoesNotExist):
        store.get("nothing")

    assert store.get("nothing", raise_on_nonexist=False) is None

    store = Store(uri="s3://anystore", raise_on_nonexist=False)
    assert store.get("nothing") is None

    # initialize (take env vars into account)
    store = get_store()
    assert store.uri == "s3://anystore/another-store"
    assert get_store(uri="foo").uri.endswith("foo")

    store = Store.from_json_uri(fixtures_path / "store.json")
    assert store.uri == "file:///tmp/cache"

    # put into not yet existing sub paths
    store = Store(uri="s3://anystore", raise_on_nonexist=False)
    store.put("foo/bar/baz", 1)
    assert store.get("foo/bar/baz") == 1

    store = Store(uri=tmp_path / "foo", raise_on_nonexist=False)
    store.put("/bar/baz", 1)
    assert (tmp_path / "foo/bar/baz").exists()
    assert store.get("/bar/baz") == 1
