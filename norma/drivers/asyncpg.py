import asyncio
import contextlib
from typing import AsyncIterator

import asyncpg
import orjson
import typic


from norma import protos


class AsyncPGConnector(protos.ConnectorProtocol[asyncpg.Record]):
    """A simple connector for asyncpg."""

    TRANSIENT = (
        asyncpg.DeadlockDetectedError,
        asyncpg.TooManyConnectionsError,
        asyncpg.PostgresConnectionError,
    )

    __slots__ = (
        "dsn",
        "pool",
        "initialized",
        "_lock",
    )

    def __init__(self, dsn: str, *, pool: asyncpg.pool.Pool = None, **connect_kwargs):
        self.dsn = dsn
        self.pool: asyncpg.pool.Pool = pool or create_pool(dsn, **connect_kwargs)
        self.initialized = False
        self._lock = asyncio.Lock()

    def __repr__(self):
        dsn, initialized, open = self.dsn, self.initialized, self.open
        return f"<{self.__class__.__name__} {dsn=} {initialized=} {open=}>"

    async def initialize(self):
        async with self._lock:
            if not self.initialized:
                await self.pool
                self.initialized = True

    @contextlib.asynccontextmanager
    async def connection(
        self, *, timeout: int = 10, c: asyncpg.Connection = None
    ) -> AsyncIterator[asyncpg.Connection]:
        await self.initialize()
        if c:
            yield c
        else:
            async with self.pool.acquire(timeout=timeout) as conn:
                yield conn

    @contextlib.asynccontextmanager
    async def transaction(
        self, *, connection: asyncpg.Connection = None, rollback: bool = False
    ) -> AsyncIterator[asyncpg.Connection]:
        async with self.connection(c=connection) as conn:
            if rollback:
                t: asyncpg.Transaction = conn.transaction()
                await t.start()
                yield conn
                await t.rollback()
            else:
                async with conn.transaction():
                    yield conn

    async def close(self, timeout: int = 10):
        await asyncio.wait_for(self.pool.close(), timeout=timeout)

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


async def _init_connection(connection: asyncpg.Connection):
    await connection.set_type_codec(
        "jsonb",
        # orjson encodes to binary, but libpq (the c bindings for postgres)
        # can't write binary data to JSONB columns.
        # https://github.com/lib/pq/issues/528
        # This is still orders of magnitude faster than any other lib.
        encoder=lambda o: orjson.dumps(o, default=typic.primitive).decode("utf8"),
        decoder=orjson.loads,
        schema="pg_catalog",
    )


def create_pool(
    dsn: str, *, loop: asyncio.AbstractEventLoop = None, **kwargs
) -> protos.ConnectorProtocol[asyncpg.Record]:
    kwargs.setdefault("init", _init_connection)
    kwargs.setdefault("loop", loop)
    return asyncpg.create_pool(dsn, **kwargs)
