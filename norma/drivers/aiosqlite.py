import asyncio
import contextlib
from typing import AsyncContextManager

import aiosqlite
import orjson
import typic

from norma import protos


class AIOSQLiteConnector:
    """A simple connector for aiosqlite."""

    TRANSIENT = (aiosqlite.OperationalError,)

    __slots__ = (
        "dsn",
        "options",
        "initialized",
        "__lock",
    )

    def __init__(self, dsn: str, **options):
        self.dsn = dsn
        self.initialized = False
        self.options = options
        self.__lock = None

    def __repr__(self):
        dsn, initialized, open = self.dsn, self.initialized, self.open
        return f"<{self.__class__.__name__} {dsn=} {initialized=} {open=}>"

    @property
    def _lock(self) -> asyncio.Lock:
        if self.__lock is None:
            self.__lock = asyncio.Lock()
        return self.__lock

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
    ) -> AsyncContextManager[aiosqlite.Connection]:
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
    def transaction(
        self, *, connection: aiosqlite.Connection = None
    ) -> AsyncContextManager[aiosqlite.Connection]:
        conn: aiosqlite.Connection
        with self.connection(c=connection) as conn:
            yield conn
            await conn.commit()

    async def close(self, timeout: int = 10):
        async with self._lock:
            self.initialized = False

    @property
    def open(self) -> bool:
        return self.initialized
