from __future__ import annotations

import asyncio
import contextlib
import threading
from typing import (
    Any,
    AsyncIterator,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import psycopg
import psycopg.rows as pgrows
import psycopg.types.array as pgarray
import psycopg.types.json as pgjson
import psycopg_pool as pgpool
import typic

from yesql.core import parse, support, types
from yesql.core.drivers import base

_T = TypeVar("_T")

__all__ = (
    "AsyncPsycoPGQueryExecutor",
    "PsycoPGQueryExecutor",
    "PsycoPGPoolSettings",
    "PsycoPGConnectionSettings",
    "create_async_pool",
    "create_sync_pool",
)


_TRANSIENT_ERRORS = (
    psycopg.OperationalError,
    psycopg.InternalError,
    pgpool.PoolTimeout,
    pgpool.TooManyRequests,
)


class AsyncPsycoPGQueryExecutor(base.BaseQueryExecutor[psycopg.AsyncConnection]):
    __driver__: str = "psycopg"
    pool: pgpool.AsyncConnectionPool | None

    TRANSIENT = _TRANSIENT_ERRORS

    def __init__(
        self,
        *,
        pool: pgpool.AsyncConnectionPool = None,
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
            self.pool = create_async_pool(**self.pool_kwargs)
            self.pool.kwargs.setdefault("row_factory", pgrows.namedtuple_row)
            await self.pool.wait(timeout=self.pool_kwargs.get("timeout", 30))

    async def teardown(self, *, timeout: int = 10):
        if not self.managed or not self.pool:
            return
        async with self._lock:
            if not self.managed or not self.pool:
                return
            try:
                await self.pool.close(timeout=timeout)
            finally:
                self.pool = None

    @contextlib.asynccontextmanager
    async def connection(
        self, *, timeout: float = 10, connection: psycopg.AsyncConnection = None
    ) -> AsyncIterator[psycopg.AsyncConnection]:
        await self.initialize()
        if connection:
            yield connection
        else:
            async with self.pool.connection(timeout=timeout) as conn:
                yield conn
                await conn.rollback()

    @contextlib.asynccontextmanager
    async def transaction(
        self,
        *,
        timeout: float = 10,
        connection: psycopg.AsyncConnection = None,
        rollback: bool = False,
        savepoint_name: Optional[str] = None,
    ) -> AsyncIterator[psycopg.AsyncConnection]:
        conn: psycopg.AsyncConnection
        async with self.connection(timeout=timeout, connection=connection) as conn:
            async with conn.transaction(
                savepoint_name=savepoint_name, force_rollback=rollback
            ):
                yield conn

    @support.retry
    async def many(
        self,
        query: parse.QueryDatum,
        *args,
        connection: psycopg.AsyncConnection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        deserializer: base.DeserializerT[_T] | None = None,
        **params,
    ):
        await self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: psycopg.AsyncConnection
        async with ctx as c:
            async with (
                await c.execute(query=query.sql, params=args or params)
            ) as cursor:
                results = await cursor.fetchall()
                if results and deserializer:
                    return deserializer(results)
                return results

    @support.retry_cursor
    @contextlib.asynccontextmanager
    async def many_cursor(
        self,
        query: parse.QueryDatum,
        *args,
        connection: psycopg.AsyncConnection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **params,
    ) -> AsyncIterator[psycopg.AsyncCursor]:
        await self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        async with ctx as connection:
            yield await connection.execute(query=query.sql, params=args or params)

    @support.retry
    async def one(
        self,
        query: parse.QueryDatum,
        *args,
        connection: psycopg.AsyncConnection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        deserializer: base.DeserializerT[_T] | None = None,
        **params,
    ):
        await self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: psycopg.AsyncConnection
        async with ctx as c:
            async with (
                await c.execute(query=query.sql, params=args or params)
            ) as cursor:
                result = await cursor.fetchone()
                if result and deserializer:
                    return deserializer(result)
                return result

    @support.retry
    async def scalar(
        self,
        query: parse.QueryDatum,
        *args,
        connection: psycopg.AsyncConnection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **params,
    ):
        await self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: psycopg.AsyncConnection
        async with ctx as c:
            async with (
                await c.execute(query=query.sql, params=args or params)
            ) as cursor:
                val = await cursor.fetchone()
                if val:
                    return val[0]
                return

    @support.retry
    async def multi(
        self,
        query: parse.QueryDatum,
        *,
        params: Iterable[Union[Sequence, Mapping[str, Any]]],
        connection: psycopg.AsyncConnection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        returns: bool = False,
        deserializer: base.DeserializerT[_T] | None = None,
    ):
        await self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: psycopg.AsyncConnection
        async with ctx as c:
            cursor: psycopg.AsyncCursor
            async with c.cursor() as cursor:
                await cursor.executemany(query=query.sql, params_seq=params)
                if not returns:
                    return cursor.rowcount or cursor.statusmessage

                out: list[_T] = []
                nextset = cursor.nextset
                fetchall = cursor.fetchall
                extend = out.extend
                extend(await fetchall())
                while nextset():
                    extend(await fetchall())
                if out and deserializer:
                    return deserializer(out)
                return out

    @support.retry_cursor
    @contextlib.asynccontextmanager
    async def multi_cursor(
        self,
        query: parse.QueryDatum,
        *,
        params: Iterable[Union[Sequence, Mapping[str, Any]]],
        connection: psycopg.AsyncConnection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
    ):
        await self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: psycopg.AsyncConnection
        async with ctx as c:
            cursor: psycopg.AsyncCursor
            async with c.cursor() as cursor:
                yield await cursor.executemany(query=query.sql, params_seq=params)

    @support.retry
    async def affected(
        self,
        query: parse.QueryDatum,
        *args,
        connection: types.ConnectionT = None,
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
        c: psycopg.AsyncConnection
        async with ctx as c:
            async with (
                await c.execute(query=query.sql, params=args or kwargs)
            ) as cursor:
                return cursor.rowcount


class PsycoPGQueryExecutor(base.BaseQueryExecutor[psycopg.Connection]):
    __driver__: str = "psycopg"
    pool: pgpool.ConnectionPool | None

    TRANSIENT = _TRANSIENT_ERRORS

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.teardown()

    def __init__(
        self,
        *,
        pool: pgpool.ConnectionPool = None,
        **pool_kwargs,
    ):
        super().__init__(pool=pool, **pool_kwargs)
        self._lock = threading.Lock()

    def initialize(self):
        if self.pool:
            return
        with self._lock:
            if self.pool:
                return
            self.pool = create_sync_pool(**self.pool_kwargs)
            self.pool.kwargs.setdefault("row_factory", pgrows.namedtuple_row)
            # FIXME: Sporadically times out if we try to eagerly wait for pool to fill.
            # self.pool.wait()

    def teardown(self, *, timeout: int = 10):
        if not self.managed or not self.pool:
            return
        with self._lock:
            if not self.managed or not self.pool:
                return
            try:
                self.pool.close(timeout=timeout)
            finally:
                self.pool = None

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
                conn.rollback()

    @contextlib.contextmanager
    def transaction(
        self,
        *,
        timeout: float = 10,
        connection: psycopg.Connection = None,
        rollback: bool = False,
        savepoint_name: Optional[str] = None,
        **_,
    ) -> Iterator[psycopg.Connection]:
        conn: psycopg.Connection
        with self.connection(timeout=timeout, connection=connection) as conn:
            with conn.transaction(
                savepoint_name=savepoint_name, force_rollback=rollback
            ):
                yield conn

    @support.retry
    def many(
        self,
        query: parse.QueryDatum,
        *args,
        connection: psycopg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        deserializer: base.DeserializerT[_T] | None = None,
        **params,
    ):
        self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: psycopg.Connection
        with ctx as c:
            with c.execute(query=query.sql, params=args or params) as cursor:
                results = cursor.fetchall()
                if results and deserializer:
                    return deserializer(results)
                return results

    @support.retry_cursor
    @contextlib.contextmanager
    def many_cursor(
        self,
        query: parse.QueryDatum,
        *args,
        connection: psycopg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **params,
    ) -> Iterator[psycopg.Cursor]:
        self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        with ctx as connection:
            yield connection.execute(query=query.sql, params=args or params)

    @support.retry
    def one(
        self,
        query: parse.QueryDatum,
        *args,
        connection: psycopg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        deserializer: base.DeserializerT[_T] | None = None,
        **params,
    ):
        self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: psycopg.Connection
        with ctx as c:
            with c.execute(query=query.sql, params=args or params) as cursor:
                result = cursor.fetchone()
                if result and deserializer:
                    return deserializer(result)
                return result

    @support.retry
    def scalar(
        self,
        query: parse.QueryDatum,
        *args,
        connection: psycopg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **params,
    ):
        self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: psycopg.Connection
        with ctx as c:
            with c.execute(query=query.sql, params=args or params) as cursor:
                val = cursor.fetchone()
                if val:
                    return val[0]
                return

    @support.retry
    def multi(
        self,
        query: parse.QueryDatum,
        *,
        params: Iterable[Union[Sequence, Mapping[str, Any]]],
        connection: psycopg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        returns: bool = False,
        deserializer: base.DeserializerT[_T] | None = None,
    ):
        self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: psycopg.Connection
        with ctx as c:
            cursor: psycopg.Cursor
            with c.cursor() as cursor:
                cursor.executemany(query=query.sql, params_seq=params)
                if not returns:
                    return cursor.rowcount or cursor.statusmessage

                out: list[_T] = []
                nextset = cursor.nextset
                fetchall = cursor.fetchall
                extend = out.extend
                extend(fetchall())
                while nextset():
                    extend(fetchall())
                if out and deserializer:
                    return deserializer(out)
                return out

    @support.retry_cursor
    @contextlib.asynccontextmanager
    def multi_cursor(
        self,
        query: parse.QueryDatum,
        *,
        params: Iterable[Union[Sequence, Mapping[str, Any]]],
        connection: psycopg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        returns: bool = True,
    ):
        self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: psycopg.Connection
        with ctx as c:
            cursor: psycopg.Cursor
            with c.cursor() as cursor:
                yield cursor.executemany(  # type: ignore[func-returns-value]
                    query=query.sql, params_seq=params
                )

    @support.retry
    def affected(
        self,
        query: parse.QueryDatum,
        *args,
        connection: psycopg.Connection = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **kwargs,
    ):
        self.initialize()
        if transaction:
            ctx = self.transaction(
                timeout=timeout, connection=connection, rollback=rollback
            )
        else:
            ctx = self.connection(timeout=timeout, connection=connection)
        c: psycopg.Connection
        with ctx as c:
            with c.execute(query=query.sql, params=args or kwargs) as cursor:
                return cursor.rowcount


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


def create_sync_pool(**overrides) -> pgpool.ConnectionPool:
    pool_kwargs = _get_environ(**overrides)
    return pgpool.ConnectionPool(**pool_kwargs)


def create_async_pool(**overrides) -> pgpool.AsyncConnectionPool:
    pool_kwargs = _get_environ(**overrides)
    return pgpool.AsyncConnectionPool(**pool_kwargs)


def _get_environ(**overrides) -> dict:
    pool_settings = PsycoPGPoolSettings()
    connect_settings = PsycoPGConnectionSettings()
    pool_fields = {k for k, v in pool_settings}
    conn_fields = {k for k, v in connect_settings}
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
    return pool_kwargs


def _init_psycopg():
    # Use a faster loader for JSON serdes
    pgjson.set_json_dumps(support.dumps)
    pgjson.set_json_loads(support.loads)
    # Register `set()` type as an array type.
    psycopg.adapters.register_dumper(set, pgarray.ListBinaryDumper)
    psycopg.adapters.register_dumper(set, pgarray.ListDumper)


_init_psycopg()  # naughty naughty...
