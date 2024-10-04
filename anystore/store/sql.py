from datetime import timedelta, datetime
from functools import cache
from operator import and_, not_
from typing import Generator, Optional, Union

from banal import ensure_dict
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    LargeBinary,
    MetaData,
    Table,
    Unicode,
    create_engine,
    delete,
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
from anystore.store.base import BaseStats, BaseStore, VirtualIOMixin
from anystore.types import Value
from anystore.util import join_relpaths


settings = SqlSettings()

Conn = Connection
Connish = Optional[Connection]


@cache
def get_engine(url: str, **kwargs) -> Engine:
    if "pool_size" not in kwargs:
        kwargs["pool_size"] = settings.pool_size
    return create_engine(url, **kwargs)


# @cache
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
        Column("ttl", Integer(), nullable=True),
        # extend_existing=True,  # FIXME?
    )


class SqlStore(VirtualIOMixin, BaseStore):
    _engine: Engine | None = None
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
        self._engine = engine
        self._conn = engine.connect()
        self._sqlite = "sqlite" in engine.name.lower()

    def _write(self, key: str, value: Value, **kwargs) -> None:
        if not isinstance(value, bytes):
            value = value.encode()
        ttl = kwargs.pop("ttl", None) or None
        # FIXME on conflict / on duplicate key
        exists = select(self._table).where(self._table.c.key == key)
        if self._conn.execute(exists).first():
            stmt = (
                update(self._table)
                .where(self._table.c.key == key)
                .values(value=value, ttl=ttl)
            )
        else:
            stmt = insert(self._table).values(key=key, value=value, ttl=ttl)
        self._conn.execute(stmt)
        self._conn.commit()

    def _read(
        self, key: str, raise_on_nonexist: bool | None = True, **kwargs
    ) -> Value | None:
        stmt = select(self._table).where(self._table.c.key == key)
        res = self._conn.execute(stmt).first()
        if res:
            key, value, ts, ttl = res
            if ttl and ts + timedelta(seconds=ttl) < datetime.utcnow():  # FIXME
                self._delete(key)
                if raise_on_nonexist:
                    raise DoesNotExist
                return
            # mimic fs read mode:
            if kwargs.get("mode") == "r" and isinstance(value, bytes):
                value = value.decode()
            return value
        if raise_on_nonexist:
            raise DoesNotExist

    def _exists(self, key: str) -> bool:
        stmt = select(self._table).where(self._table.c.key == key)
        stmt = select(stmt.exists())
        for res in self._conn.execute(stmt).first():
            return bool(res)
        return False

    def _info(self, key: str) -> BaseStats:
        stmt = select(self._table).where(self._table.c.key == key)
        res = self._conn.execute(stmt).first()
        if res:
            key, value, ts, ttl = res
            return BaseStats(created_at=ts, size=len(value))
        raise DoesNotExist

    def _delete(self, key: str) -> None:
        stmt = delete(self._table).where(self._table.c.key == key)
        self._conn.execute(stmt)

    def _get_key_prefix(self) -> str:
        return ""

    def _iterate_keys(
        self,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        glob: str | None = None,
    ) -> Generator[str, None, None]:
        table = self._table
        key_prefix = self.get_key(prefix or "")
        key_prefix = join_relpaths(key_prefix, (glob or "*").replace("*", "%"))
        stmt = select(table.c.key).where(table.c.key.like(key_prefix))
        if exclude_prefix:
            stmt = select(table.c.key).where(
                and_(
                    table.c.key.like(key_prefix),
                    not_(table.c.key.like(f"{self.get_key(exclude_prefix)}%")),
                )
            )
        with self._engine.connect() as conn:
            conn = conn.execution_options(stream_results=True)
            cursor = conn.execute(stmt)
            while rows := cursor.fetchmany(10_000):
                for row in rows:
                    yield row[0]
