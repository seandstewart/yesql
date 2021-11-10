from __future__ import annotations

import asyncio
import contextlib
import contextvars
from typing import AsyncIterator, Optional

import asyncpg
import asyncpg.transaction
import typic

from norma.core import types, support

LOCK: contextvars.ContextVar[Optional[asyncio.Lock]] = contextvars.ContextVar(
    "pg_lock", default=None
)
CONNECTOR: contextvars.ContextVar[Optional[AsyncPGConnector]] = contextvars.ContextVar(
    "pg_connector", default=None
)

__all__ = (
    "AsyncPGConnector",
    "AsyncPGConnectionSettings",
    "AsyncPGPoolSettings",
    "connector",
    "teardown",
)


async def connector(**pool_kwargs) -> AsyncPGConnector:
    """A high-level connector factory which uses context-local state."""
    async with _lock():
        if (conn := CONNECTOR.get()) is None:
            conn = AsyncPGConnector(**pool_kwargs)
            CONNECTOR.set(conn)
    await conn.initialize()
    return conn


async def teardown():
    if (conn := CONNECTOR.get()) is not None:
        await conn.close()


class AsyncPGConnector(types.AsyncConnectorProtocolT[asyncpg.Connection]):
    """A simple connector for asyncpg."""

    TRANSIENT = (
        asyncpg.DeadlockDetectedError,
        asyncpg.TooManyConnectionsError,
        asyncpg.PostgresConnectionError,
    )

    __slots__ = (
        "pool",
        "initialized",
        "pool_kwargs",
    )

    def __init__(self, *, pool: asyncpg.Pool = None, **pool_kwargs):
        self.pool: asyncpg.Pool = pool or create_pool(**pool_kwargs)
        self.initialized = False
        self.pool_kwargs = pool_kwargs

    def __repr__(self):
        initialized, open = self.initialized, self.open
        return f"<{self.__class__.__name__} {initialized=} {open=}>"

    async def initialize(self):
        if self.initialized:
            return
        async with _lock():
            await self.pool.__aenter__()
            self.initialized = True

    @contextlib.asynccontextmanager
    async def connection(
        self, *, timeout: float = 10, connection: asyncpg.Connection = None
    ) -> AsyncIterator[asyncpg.Connection]:
        await self.initialize()
        if connection:
            yield connection
        else:
            async with self.pool.acquire(timeout=timeout) as conn:
                yield conn

    @contextlib.asynccontextmanager
    async def transaction(
        self,
        *,
        timeout: float = 10,
        connection: asyncpg.Connection = None,
        rollback: bool = False,
        isolation: Optional[str] = None,
        readonly: bool = False,
        deferrable: bool = False,
    ) -> AsyncIterator[asyncpg.Connection]:
        conn: asyncpg.Connection
        async with self.connection(timeout=timeout, connection=connection) as conn:
            tctx = RollbackTransaction if rollback else asyncpg.transaction.Transaction
            async with tctx(conn, isolation, readonly, deferrable):
                yield conn

    async def close(self, timeout: float = 10):
        async with _lock():
            try:
                await asyncio.wait_for(self.pool.close(), timeout=timeout)
            except asyncio.TimeoutError:
                self.pool.terminate()

    @property
    def open(self) -> bool:
        return not self.pool._closed

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


def _lock() -> asyncio.Lock:
    if (lock := LOCK.get()) is None:
        lock = asyncio.Lock()
        LOCK.set(lock)
    return lock


class RollbackTransaction:
    """A transaction proxy which rolls back the owned transaction on exit."""

    __slots__ = ("transaction",)

    def __init__(
        self,
        connection: asyncpg.Connection,
        isolation: Optional[str] = None,
        readonly: bool = False,
        deferrable: bool = False,
    ):
        self.transaction = connection.transaction(
            isolation=isolation,
            readonly=readonly,
            deferrable=deferrable,
        )

    def __aenter__(self):
        return self.transaction.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            exc_type = _Rollback
            exc_val = _Rollback("Client-requested rollback.")
            try:
                await self.transaction.__aexit__(exc_type, exc_val, exc_tb)
            except asyncpg.InterfaceError:
                # The transaction is either closed or errored already. Move on.
                pass
        return await self.transaction.__aexit__(exc_type, exc_val, exc_tb)


class _Rollback(Exception):
    """A fake exception to pass on to the asyncpg to trigger a rollback on context exit."""

    ...


@typic.settings(prefix="POSTGRES_POOL_", aliases={"database_url": "postgres_pool_dsn"})
class AsyncPGPoolSettings:
    """Settings to pass into the asyncpg pool constructor."""

    dsn: Optional[typic.DSN] = None
    min_size: Optional[int] = None
    max_size: Optional[int] = None
    max_queries: Optional[int] = None
    max_inactive_connection_lifetime: Optional[float] = None


@typic.settings(
    prefix="POSTGRES_CONNECTION_", aliases={"database_url": "postgres_connection_dsn"}
)
class AsyncPGConnectionSettings:
    """Settings to pass into the asyncpg connection constructor."""

    dsn: Optional[typic.DSN] = None
    host: Optional[str] = None
    port: Optional[str] = None
    user: Optional[str] = None
    password: Optional[typic.SecretStr] = None
    passfile: Optional[typic.SecretStr] = None
    database: Optional[str] = None
    timeout: Optional[float] = None
    statement_cache_size: Optional[int] = None
    max_cached_statement_lifetime: Optional[int] = None
    max_cacheable_statement_size: Optional[int] = None
    command_timeout: Optional[float] = None
    ssl: Optional[str] = None


def create_pool(**overrides) -> asyncpg.Pool:
    pool_settings = AsyncPGPoolSettings()
    connect_settings = AsyncPGConnectionSettings()
    kwargs = {k: v for k, v in connect_settings if v is not None}
    kwargs.update((k, v) for k, v in pool_settings if v is not None)
    kwargs.update(overrides)
    kwargs.setdefault("init", _init_connection)
    return asyncpg.create_pool(**kwargs)


async def _init_connection(connection: asyncpg.Connection):
    await connection.set_type_codec(
        "jsonb",
        encoder=support.dumps,
        decoder=support.loads,
        schema="pg_catalog",
    )
    await connection.set_type_codec(
        "json",
        encoder=support.dumps,
        decoder=support.loads,
        schema="pg_catalog",
    )
