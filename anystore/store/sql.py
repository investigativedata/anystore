from contextlib import contextmanager
from functools import cache
from typing import Generator, Optional, Union

from banal import ensure_dict
from sqlalchemy import (
    Column,
    DateTime,
    LargeBinary,
    MetaData,
    Table,
    Unicode,
    create_engine,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as psql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine

from anystore.exceptions import DoesNotExist
from anystore.settings import SqlSettings
from anystore.store.base import BaseStore
from anystore.types import Uri, Value


settings = SqlSettings()

Conn = Connection
Connish = Optional[Connection]


@cache
def get_engine(url: str, **kwargs) -> Engine:
    if "pool_size" not in kwargs:
        kwargs["pool_size"] = settings.pool_size
    return create_engine(url, **kwargs)


@cache
def get_metadata() -> MetaData:
    return MetaData()


Insert = Union[type(sqlite_insert), type(mysql_insert), type(psql_insert)]


def get_insert(engine: Engine) -> Insert:
    if engine.dialect.name == "sqlite":
        return sqlite_insert
    if engine.dialect.name == "mysql":
        return mysql_insert
    if engine.dialect.name in ("postgresql", "postgres"):
        return psql_insert
    raise RuntimeError("Unsupported database engine: %s" % engine.dialect.name)


def make_table(name: str, metadata: MetaData) -> Table:
    return Table(
        name,
        metadata,
        Column("key", Unicode(), primary_key=True, unique=True, index=True),
        Column("value", LargeBinary(), nullable=True),
        Column(
            "timestamp",
            DateTime(timezone=True),
            server_default=func.now(),
        ),
    )


class SqlStore(BaseStore):
    _conn: Connection | None = None
    _insert: Insert | None = None
    _table: Table | None = None
    _sqlite: bool | None = True

    def __init__(self, **data):
        super().__init__(**data)
        backend_config = ensure_dict(self.backend_config)
        engine_kwargs = ensure_dict(backend_config.get("engine_kwargs"))
        metadata = get_metadata()
        engine = get_engine(self.uri, **engine_kwargs)
        table = backend_config.get("table") or settings.table
        table = make_table(table, metadata)
        metadata.create_all(engine, tables=[table], checkfirst=True)
        self._insert = get_insert(engine)
        self._table = table
        self._conn = engine.connect()
        self._sqlite = "sqlite" in engine.name.lower()

    def _write(self, key: Uri, value: Value, **kwargs) -> None:
        key = str(key)
        # FIXME on conflict / on duplicate key
        exists = select(self._table).where(self._table.c.key == key)
        if self._conn.execute(exists).first():
            stmt = (
                update(self._table)
                .where(self._table.c.key == key)
                .values(value=value)
            )
        else:
            stmt = insert(self._table).values(key=key, value=value)
        self._conn.execute(stmt)
        self._conn.commit()

    def _read(self, key: Uri, raise_on_nonexist: bool | None = True, **kwargs) -> Value:
        key = str(key)
        stmt = select(self._table).where(self._table.c.key == key)
        res = self._conn.execute(stmt).first()
        if res:
            res = res[1]
            # mimic fs read mode:
            if kwargs.get("mode") == "r" and isinstance(res, bytes):
                res = res.decode()
            return res
        if raise_on_nonexist:
            raise DoesNotExist

    def _get_key_prefix(self) -> str:
        return ""
