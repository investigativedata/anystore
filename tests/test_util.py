import pytest
from pathlib import Path

from anystore import util


def test_util_clean_dict():
    assert util.clean_dict({}) == {}
    assert util.clean_dict(None) == {}
    assert util.clean_dict("") == {}
    assert util.clean_dict({"a": "b"}) == {"a": "b"}
    assert util.clean_dict({1: 2}) == {"1": 2}
    assert util.clean_dict({"a": None}) == {}
    assert util.clean_dict({"a": ""}) == {}
    assert util.clean_dict({"a": {1: 2}}) == {"a": {"1": 2}}
    assert util.clean_dict({"a": {"b": ""}}) == {}


def test_util_ensure_uri():
    assert util.ensure_uri("https://example.com") == "https://example.com"
    assert util.ensure_uri("s3://example.com") == "s3://example.com"
    assert util.ensure_uri("foo://example.com") == "foo://example.com"
    assert util.ensure_uri("-") == "-"
    assert util.ensure_uri("./foo").startswith("file:///")
    assert util.ensure_uri(Path("./foo")).startswith("file:///")

    with pytest.raises(ValueError):
        assert util.ensure_uri("")
    with pytest.raises(ValueError):
        assert util.ensure_uri(None)
    with pytest.raises(ValueError):
        assert util.ensure_uri(" ")
