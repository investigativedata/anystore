from typer.testing import CliRunner

from anystore.cli import cli
from anystore import __version__


runner = CliRunner()


def test_cli(tmp_path, fixtures_path):
    res = runner.invoke(cli, "--help")
    assert res.exit_code == 0

    res = runner.invoke(cli, ["--store", str(tmp_path), "set", "foo", "bar"])
    assert res.exit_code == 0
    res = runner.invoke(cli, ["--store", str(tmp_path), "get", "foo"])
    assert res.exit_code == 0
    assert res.stdout == "bar"

    res = runner.invoke(cli, ["--store", "s3://foo/bar", "set", "foo", "bar"])
    assert res.exit_code == 1

    res = runner.invoke(cli, ["io", "-i", str(fixtures_path / "lorem.txt")])
    assert res.exit_code == 0
    assert res.stdout.startswith("Lorem ")

    res = runner.invoke(cli, "--version")
    assert res.exit_code == 0
    assert res.stdout.strip() == __version__
