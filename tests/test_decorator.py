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
    assert store.get(key) == "data"

    # store as arg, custom key func
    @anycache(store=store, key_func=lambda *args, **kwargs: args[0].upper())
    def get_data2(*args, **kwargs):
        return "data2"

    assert store.get("X") is None
    assert get_data2("x") == "data2"
    assert store.get("X") == "data2"
