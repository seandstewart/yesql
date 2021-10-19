from __future__ import annotations

import contextlib
import inspect
import itertools
from typing import (
    overload,
    Type,
    TypeVar,
    cast,
    Literal,
    Iterable,
    Mapping,
    Iterator,
    Optional,
    AsyncIterator,
)

from aiosql.types import QueryFn

from . import support, types, inspection

_MT = TypeVar("_MT")


def bootstrap(
    cls: Type[types.ServiceProtocolT[_MT]], func: QueryFn
) -> types.QueryMethodProtocolT[_MT]:
    """Given a ServiceProtocol and a Query function, get a "bootstrapped" method.

    This will inspect the query function and create a valid method for a ServiceProtocol.

    Notes:
        If the method is "scalar" (e.g., no return or returns a primitive value),
        the method will not attempt to coerce the result into the model bound to your
        Service.

        If the method is a "bulk" method, the method will assume that multiple records
        may be returned, and coerce them to the model if it is not also "scalar".

        If the method is a "persist" method (an insert or update) the resulting
        method will either accept an instance of your model (or models if also "bulk"),
        or raw keywords (or an iterable of mappings).
    """
    scalar = cast(
        Literal[True, False],
        bool(
            func.__name__ in cls.metadata.__scalar_queries__
            or inspection.isscalar(func)
        ),
    )
    bulk = cast(Literal[True, False], bool(not scalar and inspection.isbulk(func)))
    run_query: types.QueryMethodProtocolT[_MT]
    iscursor = func.__name__.endswith("_cursor")
    if iscursor:
        run_query = _bootstrap_cursor(func, scalar=scalar)  # type: ignore

    elif inspection.ispersist(func):
        run_query = _bootstrap_persist(func, scalar=scalar, bulk=bulk)  # type: ignore

    else:
        run_query = _bootstrap_default(func, scalar=scalar, bulk=bulk)  # type: ignore

    run_query.__name__ = func.__name__
    run_query.__doc__ = func.__doc__
    run_query.__qualname__ = f"{cls.__name__}.{func.__name__}"
    run_query.__module__ = cls.__module__
    run_query.__queryfn__ = func
    return run_query


def _bootstrap_cursor(func: QueryFn, *, scalar: bool) -> types.CursorMethodProtocolT:
    ufunc = inspect.unwrap(func)
    isaio = inspect.isasyncgenfunction(ufunc)
    if scalar and isaio:

        @support.retry_cursor
        @contextlib.asynccontextmanager
        async def run_scalar_query_cursor(
            self: types.AsyncServiceProtocolT,
            *args,
            connection: types.ConnectionT = None,
            coerce: bool = False,
            **kwargs,
        ):
            async with self.connector.connection(c=connection) as c:
                async with func(c, *args, **kwargs) as cursor:
                    yield await cursor

        return cast(types.CursorMethodProtocolT, run_scalar_query_cursor)

    if scalar:

        @support.retry_cursor
        @contextlib.contextmanager
        def run_scalar_query_cursor(
            self: types.SyncServiceProtocolT,
            *args,
            connection: types.ConnectionT = None,
            coerce: bool = False,
            **kwargs,
        ):
            with self.connector.connection(c=connection) as c:
                with func(c, *args, **kwargs) as cursor:
                    yield cursor

        return cast(types.CursorMethodProtocolT, run_scalar_query_cursor)

    if isaio:

        @support.retry_cursor
        @contextlib.asynccontextmanager
        async def run_query_cursor(
            self: types.AsyncServiceProtocolT,
            *args,
            connection: types.ConnectionT = None,
            coerce: bool = True,
            **kwargs,
        ):
            async with self.connector.connection(c=connection) as c:
                async with func(c, *args, **kwargs) as factory:
                    cursor = await factory
                    yield AsyncCoercingCursor(self, cursor) if coerce else cursor

        return cast(types.CursorMethodProtocolT, run_query_cursor)

    else:

        @support.retry_cursor
        @contextlib.contextmanager
        def run_query_cursor(
            self: types.SyncServiceProtocolT,
            *args,
            connection: types.ConnectionT = None,
            coerce: bool = True,
            **kwargs,
        ):
            with self.connector.connection(c=connection) as c:
                with func(c, *args, **kwargs) as cursor:
                    yield SyncCoercingCursor(self, cursor) if coerce else cursor

        return cast(types.CursorMethodProtocolT, run_query_cursor)


@overload
def _bootstrap_persist(
    func: QueryFn, *, scalar: Literal[False], bulk: Literal[False]
) -> types.ModelPersistProtocolT:
    ...


@overload
def _bootstrap_persist(
    func: QueryFn, *, scalar: Literal[True], bulk: Literal[False]
) -> types.ScalarPersistProtocolT:
    ...


@overload
def _bootstrap_persist(
    func: QueryFn, *, scalar: Literal[True], bulk: Literal[True]
) -> types.ScalarBulkPersistProtocolT:
    ...


@overload
def _bootstrap_persist(
    func: QueryFn, *, scalar: Literal[False], bulk: Literal[True]
) -> types.ScalarBulkMethodProtocolT:
    ...


def _bootstrap_persist(
    func: QueryFn, *, scalar: Literal[True, False], bulk: Literal[True, False]
):
    isaio = inspect.iscoroutinefunction(func)
    if bulk and isaio:

        @support.retry
        async def run_persist_query(
            self: types.AsyncServiceProtocolT,
            *__,
            connection: types.ConnectionT = None,
            models: Iterable[types.ModelT] = (),
            data: Iterable[Mapping] = (),
            **___,
        ):
            if models:
                data = itertools.chain(data, (self.get_kvs(m) for m in models))
            async with self.connector.transaction(connection=connection) as c:
                return await func(c, data)

    elif bulk:

        @support.retry
        def run_persist_query(
            self: types.SyncServiceProtocolT,
            *__,
            connection: types.ConnectionT = None,
            models: Iterable[types.ModelT] = (),
            data: Iterable[Mapping] = (),
            **___,
        ):
            if models:
                data = itertools.chain(data, (self.get_kvs(m) for m in models))
            with self.connector.transaction(connection=connection) as c:
                return func(c, data)

    elif isaio:

        @support.retry
        async def run_persist_query(
            self: types.AsyncServiceProtocolT,
            *__,
            model: types.ModelT = None,
            connection: types.ConnectionT = None,
            **kwargs,
        ):
            data = kwargs
            if model:
                data.update(self.get_kvs(model))
            async with self.connector.transaction(connection=connection) as c:
                return await func(c, **data)

    else:

        @support.retry
        def run_persist_query(
            self: types.SyncServiceProtocolT,
            *__,
            model: types.ModelT = None,
            connection: types.ConnectionT = None,
            **kwargs,
        ):
            data = kwargs
            if model:
                data.update(self.get_kvs(model))
            with self.connector.transaction(connection=connection) as c:
                return func(c, **data)

    if scalar is False:
        return support.coerceable(run_persist_query, bulk=bulk)

    return run_persist_query


@overload
def _bootstrap_default(
    func: QueryFn, *, scalar: Literal[False], bulk: Literal[False]
) -> types.ModelMethodProtocolT:
    ...


@overload
def _bootstrap_default(
    func: QueryFn, *, scalar: Literal[False], bulk: Literal[True]
) -> types.ModelBulkMethodProtocolT:
    ...


@overload
def _bootstrap_default(
    func: QueryFn, *, scalar: Literal[True], bulk: Literal[False]
) -> types.QueryMethodProtocolT:
    ...


@overload
def _bootstrap_default(
    func: QueryFn, *, scalar: Literal[True], bulk: Literal[True]
) -> types.ScalarBulkMethodProtocolT:
    ...


def _bootstrap_default(
    func: QueryFn, *, scalar: Literal[True, False], bulk: Literal[True, False]
):
    ismutate, isaio = inspection.ismutate(func), inspect.iscoroutinefunction(func)
    if ismutate and isaio:

        @support.retry
        async def run_default_query(
            self: types.AsyncServiceProtocolT,
            *args,
            connection: types.ConnectionT = None,
            **kwargs,
        ):
            async with self.connector.transaction(connection=connection) as c:
                return await func(c, *args, **kwargs)

    elif ismutate:

        @support.retry
        def run_default_query(
            self: types.SyncServiceProtocolT,
            *args,
            connection: types.ConnectionT = None,
            **kwargs,
        ):
            with self.connector.transaction(connection=connection) as c:
                return func(c, *args, **kwargs)

    elif isaio:

        @support.retry
        async def run_default_query(
            self: types.AsyncServiceProtocolT,
            *args,
            connection: types.ConnectionT = None,
            **kwargs,
        ):
            async with self.connector.connection(c=connection) as c:
                return await func(c, *args, **kwargs)

    else:

        @support.retry
        def run_default_query(
            self: types.SyncServiceProtocolT,
            *args,
            connection: types.ConnectionT = None,
            **kwargs,
        ):
            with self.connector.connection(c=connection) as c:
                return func(c, *args, **kwargs)

    if scalar is False:
        return support.coerceable(run_default_query, bulk=bulk)

    return run_default_query


class AsyncCoercingCursor(types.AsyncCursorProtocolT[_MT]):
    """A shim around a cursor which will automatically convert data to the model."""

    __slots__ = ("service", "cursor")

    def __init__(
        self,
        service: types.ServiceProtocolT,
        cursor: types.AsyncCursorProtocolT[Mapping],
    ):
        self.service = service
        self.cursor = cursor

    def __getattr__(self, item: str):
        return self.cursor.__getattribute__(item)

    async def __aiter__(self) -> AsyncIterator[_MT]:
        async for row in self.cursor:
            yield self.service.protocol(row)

    async def forward(self, n: int, *args, timeout: float = None, **kwargs):
        return await self.cursor.forward(n, *args, timeout=timeout, **kwargs)

    async def fetch(
        self, n: int, *args, timeout: float = None, **kwargs
    ) -> Iterable[_MT]:
        page = await self.cursor.fetch(n, *args, timeout=timeout, **kwargs)
        return page and self.service.bulk_protocol(({**r} for r in page))  # type: ignore

    async def fetchrow(self, *args, timeout: float = None, **kwargs) -> Optional[_MT]:
        row = await self.cursor.fetchrow(*args, timeout=timeout, **kwargs)
        return row and self.service.protocol({**row})  # type: ignore


class SyncCoercingCursor(types.SyncCursorProtocolT[_MT]):
    """A shim around a cursor which will automatically convert data to the model."""

    __slots__ = ("service", "cursor")

    def __init__(
        self,
        service: types.ServiceProtocolT,
        cursor: types.SyncCursorProtocolT[Mapping],
    ):
        self.service = service
        self.cursor = cursor

    def __getattr__(self, item: str):
        return self.cursor.__getattribute__(item)

    def __iter__(self) -> Iterator[_MT]:
        for row in self.cursor:
            yield self.service.protocol(row)

    def forward(self, n: int, *args, timeout: float = None, **kwargs):
        return self.cursor.forward(n, *args, timeout=timeout, **kwargs)

    def fetch(self, n: int, *args, timeout: float = None, **kwargs) -> Iterable[_MT]:
        page = self.cursor.fetch(n, *args, timeout=timeout, **kwargs)
        return page and self.service.bulk_protocol(({**r} for r in page))  # type: ignore

    def fetchrow(self, *args, timeout: float = None, **kwargs) -> Optional[_MT]:
        row = self.cursor.fetchrow(*args, timeout=timeout, **kwargs)
        return row and self.service.protocol({**row})  # type: ignore
