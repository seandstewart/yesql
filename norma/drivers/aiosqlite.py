import asyncio
import contextlib
from typing import AsyncIterator

import aiosqlite

from norma import protos


class AIOSQLiteConnector(protos.ConnectorProtocol[aiosqlite.Row]):
    """A ConnectorProtocol interface for aiosqlite."""

    TRANSIENT = (aiosqlite.OperationalError,)

    __slots__ = ("dsn", "options", "initialized", "_lock")

    def __init__(self, dsn: str, **options):
        self.dsn = dsn
        self.initialized = False
        self.options = options
        self._lock = asyncio.Lock()

    def __repr__(self):
        dsn, initialized, open = self.dsn, self.initialized, self.open
        return f"<{self.__class__.__name__} {dsn=} {initialized=} {open=}>"

    async def initialize(self):
        async with self._lock:
            if not self.initialized:
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
            async with aiosqlite.connect(
                self.dsn, timeout=timeout, **self.options
            ) as conn:
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
        async with self._lock:
            self.initialized = False

    @property
    def open(self) -> bool:
        return self.initialized
