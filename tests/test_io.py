import pytest
from pathlib import Path

from moto import mock_aws

from anystore.io import (
    SmartHandler,
    smart_open,
    smart_stream,
    smart_read,
    smart_write,
)
from tests.conftest import setup_s3


def test_io_read(fixtures_path: Path):
    path = fixtures_path / "lorem.txt"
    txt = smart_read(path)
    assert isinstance(txt, bytes)
    assert txt.decode().startswith("Lorem")

    txt = smart_read(path, "r")
    assert isinstance(txt, str)
    assert txt.startswith("Lorem")

    tested = False
    for ix, line in enumerate(smart_stream(path, "r")):
        if ix == 1:
            assert line.startswith("tempor")
            tested = True
            break
    assert tested


def test_io_write(tmp_path: Path):
    path = tmp_path / "lorem.txt"
    smart_write(path, b"Lorem")
    assert path.exists() and path.is_file()
    assert smart_read(path, "r") == "Lorem"


def test_io_write_stdout(capsys):
    smart_write("-", b"hello")
    captured = capsys.readouterr()
    assert captured.out == "hello"


def test_io_smart_open(tmp_path: Path, fixtures_path: Path):
    with smart_open(fixtures_path / "lorem.txt", "r") as f:
        assert f.read().startswith("Lorem")

    with smart_open(tmp_path / "foo.txt", "w") as f:
        f.write("bar")

    assert smart_read(tmp_path / "foo.txt", "r") == "bar"


@mock_aws
def test_io_generic(fixtures_path: Path):
    setup_s3()
    uri = "s3://anystore/foo"
    content = b"bar"
    smart_write(uri, content)
    assert smart_read(uri) == content

    url = "http://localhost:8000/lorem.txt"
    content = smart_read(url, mode="r")
    assert content.startswith("Lorem")

    tested = False
    for line in smart_stream(url, "r"):
        assert line.startswith("Lorem")
        tested = True
        break
    assert tested


@mock_aws
def test_io_smart_handler(fixtures_path: Path):
    with SmartHandler(fixtures_path / "lorem.txt") as h:
        line = h.readline()
        assert line.decode().startswith("Lorem")

    setup_s3()
    uri = "s3://anystore/content"
    content = b"foo"
    with SmartHandler(uri, mode="wb") as h:
        h.write(content)

    assert smart_read(uri) == content


def test_io_invalid():
    with pytest.raises(ValueError):
        smart_read("")
    with pytest.raises(ValueError):
        smart_read(None)
