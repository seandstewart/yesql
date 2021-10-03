from __future__ import annotations

import threading
import contextlib
import contextvars
from typing import Optional, Iterator

import psycopg
import psycopg_pool as pgpool
import psycopg.types.json as pgjson
import orjson
import typic

from norma import types

LOCK: contextvars.ContextVar[Optional[threading.Lock]] = contextvars.ContextVar(
    "pg_lock", default=None
)
CONNECTOR: contextvars.ContextVar[Optional[PsycoPGConnector]] = contextvars.ContextVar(
    "pg_connector", default=None
)

pgjson.set_json_dumps(lambda o: orjson.dumps(o, default=typic.primitive).decode("utf8"))
pgjson.set_json_loads(orjson.loads)


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
            self.initialized = True

    @contextlib.contextmanager
    def connection(
        self, *, timeout: int = 10, c: psycopg.Connection = None
    ) -> Iterator[psycopg.Connection]:
        self.initialize()
        if c:
            yield c
        else:
            with self.pool.connection(timeout=timeout) as conn:
                yield conn

    @contextlib.contextmanager
    def transaction(
        self, *, connection: psycopg.Connection = None, rollback: bool = False
    ) -> Iterator[psycopg.Connection]:
        conn: psycopg.Connection
        with self.connection(c=connection) as conn:
            with conn.transaction(force_rollback=rollback):
                yield conn

    def close(self, timeout: int = 10):
        with _lock():
            if self.open:
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
    min_conn: int = 0
    max_conn: int = 10
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
    kwargs = {k: v for k, v in connect_settings if v is not None}
    kwargs.update((k, v) for k, v in pool_settings if v is not None)
    kwargs.update(overrides)
    return pgpool.ConnectionPool(**kwargs)
