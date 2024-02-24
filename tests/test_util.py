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


def test_util_checksum():
    assert util.make_data_checksum("stable") == "a26dc899771c9e8503618745c4842c7d"
    assert len(util.make_data_checksum("a")) == 32
    assert len(util.make_data_checksum({"foo": "bar"})) == 32
    assert len(util.make_data_checksum(True)) == 32
    assert util.make_data_checksum(["a", 1]) != util.make_data_checksum(["a", "1"])
