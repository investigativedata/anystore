from anystore.decorators import anycache
from anystore.util import make_signature_key
from anystore.store import get_store


def test_decorator(tmp_path):
    @anycache(uri=tmp_path)
    def get_data(*args, **kwargs):
        return "data"

    store = get_store(uri=tmp_path, raise_on_nonexist=False)
    key = make_signature_key("x")
    assert store.get(key) is None
    assert get_data("x") == "data"
    assert store.get(key) == b"data"

    # store as arg, custom key func
    @anycache(store=store, key_func=lambda *args, **kwargs: args[0].upper())
    def get_data2(*args, **kwargs):
        return "data2"

    assert store.get("X") is None
    assert get_data2("x") == "data2"
    assert store.get("X") == b"data2"

    # not yet existing
    @anycache(uri=tmp_path / "foo", key_func=lambda x: x)
    def get_data3(data):
        return data

    assert get_data3("bar") == "bar"
    assert (tmp_path / "foo" / "bar").exists()

    # custom serialize function
    @anycache(uri=tmp_path, serialize_func=list)
    def get_data4(x):
        yield from range(x)

    assert get_data4(5) == [0, 1, 2, 3, 4]
    # now from cache:
    assert get_data4(5) == [0, 1, 2, 3, 4]
