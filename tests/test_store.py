import pytest
from moto import mock_s3
from anystore.exceptions import DoesNotExist

from anystore.store import Store, get_store
from tests.conftest import setup_s3


@mock_s3
def test_store(tmp_path, fixtures_path):
    setup_s3()

    # store and retrieve
    store = Store(uri=tmp_path)
    key = "test"
    store.set(key, "foo")
    assert store.get(key) == "foo"
    store.set(key, False)
    assert store.get(key) is False

    store = Store(uri="s3://anystore")
    key = "test"
    store.set(key, "foo")
    assert store.get(key) == "foo"
    store.set(key, False)
    assert store.get(key) is False

    # don't pickle "external" data
    store = Store(uri=fixtures_path, use_pickle=False)
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
