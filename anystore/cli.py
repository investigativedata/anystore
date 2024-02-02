from rich import print
from rich.console import Console
from typing import Annotated, Optional

import typer

from anystore import __version__
from anystore.io import smart_read, smart_write
from anystore.settings import Settings
from anystore.store import get_store

cli = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)
console = Console(stderr=True)

settings = Settings()

state = {"uri": settings.uri, "pickle": False}


class ErrorHandler:
    def __enter__(self):
        pass

    def __exit__(self, e, msg, _):
        if e is not None:
            console.print(f"[red][bold]{e.__name__}[/bold]: {msg}[/red]")
            raise typer.Exit(code=1)


@cli.callback(invoke_without_command=True)
def cli_store(
    version: Annotated[Optional[bool], typer.Option(..., help="Show version")] = False,
    store: Annotated[
        Optional[str], typer.Option(..., help="Store base uri")
    ] = settings.uri,
    pickle: Annotated[
        Optional[bool], typer.Option(..., help="Use pickle serializer")
    ] = False,
):
    if version:
        print(__version__)
        raise typer.Exit()
    state["uri"] = store
    state["pickle"] = pickle


@cli.command("get")
def cli_get(
    key: str,
    o: Annotated[str, typer.Option("-o")] = "-",
):
    """
    Get content of a `key` from a store
    """
    with ErrorHandler():
        S = get_store(uri=state["uri"], use_pickle=state["pickle"])
        value = S.get(key)
        mode = "w" if isinstance(value, str) else "wb"
        smart_write(o, value, mode=mode)


@cli.command("put")
def cli_put(
    key: str,
    value: Annotated[
        Optional[str],
        typer.Argument(..., help="Use this value instead of reading `-i` uri"),
    ] = None,
    i: Annotated[str, typer.Option("-i")] = "-",
):
    """
    Put content for a `key` to a store
    """
    with ErrorHandler():
        S = get_store(uri=state["uri"], use_pickle=state["pickle"])
        value = value or smart_read(i)
        S.put(key, value)


@cli.command("io")
def cli_io(
    i: Annotated[str, typer.Option("-i", help="Input uri")] = "-",
    o: Annotated[str, typer.Option("-o", help="Output uri")] = "-",
):
    """
    Generic i/o wrapper using `anystore.io.smart_read` and
    `anystore.io.smart_write` which is wrapped around `fsspec`
    """
    with ErrorHandler():
        smart_write(o, smart_read(i))
