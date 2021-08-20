from __future__ import annotations

import asyncio
import contextlib
import contextvars
from typing import AsyncIterator, Optional

import aiosqlite
import typic

from norma import protos

LOCK: contextvars.ContextVar[Optional[asyncio.Lock]] = contextvars.ContextVar(
    "sqlite_lock", default=None
)
CONNECTOR: contextvars.ContextVar[
    Optional[AIOSQLiteConnector]
] = contextvars.ContextVar("sqlite_connector", default=None)


async def connnector(**options) -> AIOSQLiteConnector:
    """A high-level connector factory which uses context-local state."""
    async with _lock():
        if (connector := CONNECTOR.get()) is None:
            connector = AIOSQLiteConnector(**options)
            CONNECTOR.set(connector)
        await connector.initialize()
        return connector


async def teardown():
    if (connector := CONNECTOR.get()) is not None:
        await connector.close()


class AIOSQLiteConnector(protos.ConnectorProtocol[aiosqlite.Connection, aiosqlite.Row]):
    """A ConnectorProtocol interface for aiosqlite."""

    TRANSIENT = (aiosqlite.OperationalError,)

    __slots__ = ("options", "initialized")

    def __init__(self, **options):
        self.initialized = False
        self.options = get_options(**options)

    def __repr__(self):
        initialized, open = self.initialized, self.open
        return f"<{self.__class__.__name__} {initialized=} {open=}>"

    async def initialize(self):
        if self.initialized:
            return

        async with _lock():
            conn: aiosqlite.Connection
            async with self.connection() as conn:
                cur: aiosqlite.Cursor = await conn.execute("SELECT 1;")
                await cur.close()
            self.initialized = True

    @contextlib.asynccontextmanager
    async def connection(
        self, *, timeout: int = 10, c: aiosqlite.Connection = None
    ) -> AsyncIterator[aiosqlite.Connection]:
        await self.initialize()
        if c:
            yield c
        else:
            options = {**self.options}
            options.update(timeout=timeout)
            async with aiosqlite.connect(**self.options) as conn:
                conn.row_factory = aiosqlite.Row
                yield conn
                if conn.in_transaction:
                    await conn.rollback()

    @contextlib.asynccontextmanager
    async def transaction(
        self, *, connection: aiosqlite.Connection = None, rollback: bool = False
    ) -> AsyncIterator[aiosqlite.Connection]:
        conn: aiosqlite.Connection
        async with self.connection(c=connection) as conn:
            yield conn
            if not rollback:
                await conn.commit()

    async def close(self, timeout: int = 10):
        async with _lock():
            self.initialized = False

    @property
    def open(self) -> bool:
        return self.initialized

    @classmethod
    def get_explain_command(cls, analyze: bool = False, format: str = None) -> str:
        return cls.EXPLAIN_PREFIX


@typic.settings(prefix="SQLITE_", aliases={"database_url": "sqlite_database"})
class AIOSQLiteSettings:
    database: Optional[typic.DSN] = None
    timeout: Optional[float] = None
    detect_types: Optional[int] = None
    isolation_level: Optional[str] = None
    check_same_thread: Optional[bool] = None
    cached_statements: Optional[int] = None
    iter_chunk_size: Optional[int] = None


def get_options(**overrides) -> dict:
    settings: AIOSQLiteSettings = AIOSQLiteSettings.transmute(overrides)
    options = {f: v for f, v in settings if v is not None}
    options.setdefault("uri", True)
    return options


def _lock() -> asyncio.Lock:
    if (lock := LOCK.get()) is None:
        lock = asyncio.Lock()
        LOCK.set(lock)
    return lock
