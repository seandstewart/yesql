from __future__ import annotations

import threading
import contextlib
import contextvars
from typing import Optional, Iterator, Union

import psycopg
import psycopg_pool as pgpool
import psycopg.types.array as pgarray
import psycopg.types.json as pgjson
import psycopg.rows as pgrows
import typic

from norma.core import types, support

LOCK: contextvars.ContextVar[Optional[threading.Lock]] = contextvars.ContextVar(
    "pg_lock", default=None
)
CONNECTOR: contextvars.ContextVar[Optional[PsycoPGConnector]] = contextvars.ContextVar(
    "pg_connector", default=None
)


def connector(**pool_kwargs) -> PsycoPGConnector:
    """A high-level connector factory which uses context-local state."""
    with _lock():
        if (conn := CONNECTOR.get()) is None:
            conn = PsycoPGConnector(**pool_kwargs)
            CONNECTOR.set(conn)
        conn.initialize()
        return conn


def teardown():
    if (conn := CONNECTOR.get()) is not None:
        conn.close()


class PsycoPGConnector(types.SyncConnectorProtocolT[psycopg.Connection]):
    """A simple connector for asyncpg."""

    TRANSIENT = (
        psycopg.OperationalError,
        psycopg.InternalError,
        pgpool.PoolTimeout,
        pgpool.TooManyRequests,
    )

    __slots__ = (
        "pool",
        "initialized",
        "pool_kwargs",
    )

    def __init__(self, *, pool: pgpool.ConnectionPool = None, **pool_kwargs):
        self.pool = pool
        self.initialized = self.pool is not None
        self.pool_kwargs = pool_kwargs

    def __repr__(self):
        initialized, open = self.initialized, self.open
        return f"<{self.__class__.__name__} {initialized=} {open=}>"

    def initialize(self):
        if self.initialized:
            return
        with _lock():
            self.pool = create_pool(**self.pool_kwargs)
            self.pool.kwargs.setdefault("row_factory", pgrows.namedtuple_row)
            self.initialized = True

    @contextlib.contextmanager
    def connection(
        self, *, timeout: float = 10, connection: psycopg.Connection = None
    ) -> Iterator[psycopg.Connection]:
        self.initialize()
        if connection:
            yield connection
        else:
            with self.pool.connection(timeout=timeout) as conn:
                yield conn

    @contextlib.contextmanager
    def transaction(
        self,
        *,
        timeout: float = 10,
        connection: psycopg.Connection = None,
        savepoint_name: Optional[str] = None,
        rollback: bool = False,
    ) -> Iterator[psycopg.Connection]:
        conn: psycopg.Connection
        with self.connection(timeout=timeout, connection=connection) as conn:
            with conn.transaction(
                savepoint_name=savepoint_name, force_rollback=rollback
            ):
                yield conn

    def close(self, timeout: float = 10):
        with _lock():
            self.pool.close(timeout=timeout)

    @property
    def open(self) -> bool:
        return not self.pool.closed

    @classmethod
    def get_explain_command(cls, analyze: bool = False, format: str = None) -> str:
        options = (
            f"{'ANALYZE, ' if analyze else ''}"
            f"{'FORMAT ' if format else ''}"
            f"{format or ''}"
        )
        if options:
            return f"{cls.EXPLAIN_PREFIX} ({options})"
        return cls.EXPLAIN_PREFIX


def _lock() -> threading.Lock:
    if (lock := LOCK.get()) is None:
        lock = threading.Lock()
        LOCK.set(lock)
    return lock


@typic.settings(
    prefix="POSTGRES_POOL_",
    aliases={
        "database_url": "postgres_pool_dsn",
        "postgres_pool_conninfo": "postgres_pool_dsn",
    },
)
class PsycoPGPoolSettings:
    """Settings to pass into the asyncpg pool constructor."""

    dsn: Optional[typic.DSN] = None
    min_size: int = 0
    max_size: int = 10
    name: Optional[str] = None
    timeout: int = 0
    max_lifetime: float = 60 * 60.0
    max_idle: float = 10 * 60.0
    reconnect_timeout: float = 5 * 60.0
    num_workers: int = 3


@typic.settings(
    prefix="POSTGRES_CONNECTION_",
    aliases={
        "database_url": "postgres_connection_dsn",
        "postgres_connection_conninfo": "postgres_connection_dsn",
    },
)
class PsycoPGConnectionSettings:
    """Settings to pass into the asyncpg connection constructor."""

    dsn: Optional[typic.DSN] = None
    dbname: Optional[str] = None
    host: Optional[str] = None
    port: Optional[str] = None
    user: Optional[str] = None
    password: Optional[typic.SecretStr] = None
    passfile: Optional[typic.SecretStr] = None
    autocommit: bool = False


def create_pool(**overrides) -> pgpool.ConnectionPool:
    pool_settings = PsycoPGPoolSettings()
    connect_settings = PsycoPGConnectionSettings()
    pool_fields = {k for k in pool_settings}
    conn_fields = {k for k in connect_settings}
    pool_kwargs = {k: v for k, v in pool_settings if v is not None}
    pool_kwargs.update(((k, v) for k, v in overrides.items() if k in pool_fields))
    connect_kwargs = {
        k: v for k, v in connect_settings if v is not None and k not in pool_kwargs
    }
    connect_kwargs.update(((k, v) for k, v in overrides.items() if k in conn_fields))
    if "dsn" in pool_kwargs:
        pool_kwargs["conninfo"] = pool_kwargs.pop("dsn")
    if "dsn" in connect_kwargs:
        dsn = connect_kwargs.pop("dsn")
        if "conninfo" not in pool_kwargs:
            connect_kwargs["conninfo"] = dsn
    pool_kwargs["kwargs"] = connect_kwargs
    return pgpool.ConnectionPool(**pool_kwargs)


class _PsycoPGCursorProxy:
    __slots__ = ("_cursor",)

    def __init__(self, cursor: Union[psycopg.Cursor, psycopg.ServerCursor]):
        self._cursor = cursor

    def __getattr__(self, item):
        return self._cursor.__getattribute__(item)

    def forward(self, n: int, *args, timeout: float = None, **kwargs):
        kwargs["value"] = n
        return self._cursor.scroll(*args, **kwargs)

    def fetch(self, n: int, *args, timeout: float = None, **kwargs):
        return self._cursor.fetchmany(n)

    def fetchrow(self, *args, timeout: float = None, **kwargs):
        return self._cursor.fetchone()


def _init_psycopg():
    # Use a faster loader for JSON serdes
    pgjson.set_json_dumps(support.dumps)
    pgjson.set_json_loads(support.loads)
    # Register `set()` type as an array type.
    psycopg.adapters.register_dumper(set, pgarray.ListBinaryDumper)
    psycopg.adapters.register_dumper(set, pgarray.ListDumper)


_init_psycopg()  # naughty naughty...
