import os
import pytest
from pathlib import Path

from anystore import util, smart_read


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


def test_util_join_uri():
    assert util.join_uri("http://example.org", "foo") == "http://example.org/foo"
    assert util.join_uri("http://example.org/", "foo") == "http://example.org/foo"
    assert util.join_uri("/tmp", "foo") == "file:///tmp/foo"
    assert util.join_uri(Path("./foo"), "bar").startswith("file:///")
    assert util.join_uri(Path("./foo"), "bar").endswith("foo/bar")
    assert util.join_uri("s3://foo/bar.pdf", "../baz.txt") == "s3://foo/baz.txt"


def test_util_checksum(tmp_path, fixtures_path):
    assert util.make_data_checksum("stable") == "b34d0813267b917b79d574726d2b0ac2e3929a87"
    assert len(util.make_data_checksum("a")) == 40
    assert len(util.make_data_checksum({"foo": "bar"})) == 40
    assert len(util.make_data_checksum(True)) == 40
    assert util.make_data_checksum(["a", 1]) != util.make_data_checksum(["a", "1"])

    os.system(f"sha1sum {fixtures_path / 'lorem.txt'} > {tmp_path / 'ch'}")
    sys_ch = smart_read(tmp_path / "ch", mode="r").split()[0]
    with open(fixtures_path / "lorem.txt", "rb") as i:
        ch = util.make_checksum(i)
    assert ch == "ed3141878ed32d8a1d583e7ce7de323118b933d3"
    assert sys_ch == ch
