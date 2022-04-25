from __future__ import annotations

import asyncio
import contextlib
from typing import (
    Any,
    AsyncIterator,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import asyncpg
import typic

from yesql.core import parse, support
from yesql.core.drivers import base

__all__ = (
    "AsyncPGQueryExecutor",
    "AsyncPGPoolSettings",
    "AsyncPGConnectionSettings",
    "create_pool",
)


_T = TypeVar("_T")


class AsyncPGQueryExecutor(base.BaseQueryExecutor):
    __driver__: str = "asyncpg"
    pool: asyncpg.Pool | None

    TRANSIENT = (
        asyncpg.DeadlockDetectedError,
        asyncpg.TooManyConnectionsError,
        asyncpg.PostgresConnectionError,
    )

    def __init__(
        self,
        *,
        pool: asyncpg.Pool = None,
        **pool_kwargs,
    ):
        super().__init__(pool=pool, **pool_kwargs)
        self._lock = asyncio.Lock()

    def __await__(self):
        return self.initialize().__await__()

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.teardown()

    async def initialize(self):
        if self.pool:
            return
        async with self._lock:
            if self.pool:
                return
            self.pool = await create_pool(**self.pool_kwargs)

    async def teardown(self, *, timeout: int = 10):
        if not self.pool or not self.managed:
            return
        async with self._lock:
            if not self.pool or not self.managed:
                return
            try:
                await asyncio.wait_for(self.pool.close(), timeout=timeout)
            except asyncio.TimeoutError:
                self.pool.terminate()
            finally:
                self.pool = None

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
            tctx = (
                RollbackTransaction(conn, isolation, readonly, deferrable)
                if rollback
                else conn.transaction(
                    isolation=isolation, readonly=readonly, deferrable=deferrable
                )
            )
            async with tctx:
                yield conn

    @support.retry
    async def many(
        self,
        query: parse.QueryDatum,
        *args,
        connection: asyncpg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        deserializer: base.DeserializerT[_T] | None = None,
        **kwargs,
    ):
        await self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: asyncpg.Connection
        async with ctx as c:
            results = await c.fetch(
                query.sql, *self._remap_kwargs(query, args, kwargs), timeout=timeout
            )
            if results and deserializer:
                return deserializer(results)
            return results

    @support.retry_cursor
    @contextlib.asynccontextmanager
    async def many_cursor(
        self,
        query: parse.QueryDatum,
        *args,
        connection: asyncpg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **kwargs,
    ) -> asyncpg.connection.cursor.Cursor:
        await self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        async with ctx as connection:
            yield await connection.cursor(
                query=query.sql, *self._remap_kwargs(query, args, kwargs)
            )

    @support.retry
    async def one(
        self,
        query: parse.QueryDatum,
        *args,
        connection: asyncpg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        deserializer: Optional[base.DeserializerT[_T]] = None,
        **kwargs,
    ):
        await self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: asyncpg.Connection
        async with ctx as c:
            result = await c.fetchrow(
                query.sql, *self._remap_kwargs(query, args, kwargs), timeout=timeout
            )
            if result and deserializer:
                return deserializer(result)
            return result

    @support.retry
    async def scalar(
        self,
        query: parse.QueryDatum,
        *args,
        connection: asyncpg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **kwargs,
    ):
        await self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: asyncpg.Connection
        async with ctx as c:
            return await c.fetchval(
                query.sql, *self._remap_kwargs(query, args, kwargs), timeout=timeout
            )

    @support.retry
    async def multi(
        self,
        query: parse.QueryDatum,
        *,
        params: Iterable[Union[Sequence, Mapping[str, Any]]],
        connection: asyncpg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        returns: bool = False,
        deserializer: Optional[base.DeserializerT[_T]] = None,
    ):
        await self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: asyncpg.Connection
        async with ctx as c:
            params = [*self._remap_multi_params(query, params)]
            return await c.executemany(query.sql, params, timeout=timeout)

    async def multi_cursor(
        self,
        query: parse.QueryDatum,
        *,
        params: Iterable[Sequence | Mapping[str, Any]],
        connection: asyncpg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
    ):
        raise TypeError("asyncpg doesn't support multi-exec cursors.")

    @support.retry
    async def affected(
        self,
        query: parse.QueryDatum,
        *args,
        connection: asyncpg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **kwargs,
    ):
        await self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: asyncpg.Connection
        async with ctx as c:
            response = await c.execute(
                query.sql, *self._remap_kwargs(query, args, kwargs), timeout=timeout
            )
            return self._get_affected(response)

    def _remap_multi_params(
        self, query: parse.QueryDatum, params: Iterable[Sequence | Mapping[str, Any]]
    ):
        return (
            (*self._remap_kwargs(query, (), p),) if isinstance(p, Mapping) else p
            for p in params
        )

    @staticmethod
    def _remap_kwargs(
        query: parse.QueryDatum, args: Sequence, kwargs: Mapping[str, Any]
    ) -> Iterable:
        remapped = dict(enumerate(args, start=1))
        if kwargs:
            if not query.remapping:
                raise TypeError(
                    f"{query.name!r} cannot take keyword arguments: "
                    f"({', '.join(f'{k}={v!r}' for k, v in kwargs.items())})"
                ) from None
            remapped.update(
                (query.remapping[k], kwargs[k])
                for k in query.remapping.keys() & kwargs.keys()
            )
        yield from (v for i, v in sorted(remapped.items(), key=lambda o: o[0]))

    @staticmethod
    def _get_affected(response: str) -> int | None:
        if not response:
            return None
        affected = response.rsplit(" ", maxsplit=1)[-1]
        if affected.isdigit():
            return int(affected)
        return 0


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
                return await self.transaction.__aexit__(exc_type, exc_val, exc_tb)
            except asyncpg.InterfaceError:
                # The transaction is either closed or errored already. Move on.
                pass


class _Rollback(Exception):
    """A fake exception to pass on to the asyncpg to trigger a rollback on context exit."""

    ...


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
        encoder=support.dumpsb,
        decoder=support.loads,
        schema="pg_catalog",
        format="binary",
    )
    await connection.set_type_codec(
        "json",
        encoder=support.dumps,
        decoder=support.loads,
        schema="pg_catalog",
    )


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
