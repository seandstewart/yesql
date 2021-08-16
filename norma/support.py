from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
from typing import (
    Callable,
    Awaitable,
    Union,
    Optional,
    Type,
    Mapping,
    cast,
    Iterable,
    overload,
    Literal,
    AsyncContextManager,
)

from aiosql.types import QueryFn, SQLOperationType

from norma import protos, drivers

__all__ = (
    "coerceable",
    "get_connector_protocol",
    "middleware",
    "retry",
    "retry_cursor",
    "isbulk",
    "ismutate",
    "ispersist",
    "isscalar",
)


def middleware(*queries: str):
    """Flag to the service that this method is a 'middleware' for the target query.

    A 'middleware' method allows for pre- and post-processing of for the assigned query
    methods.

    Notes:
        Middlewares should always be a coroutine function.

    Args:
        *queries: str
            The queries for which this middelwares should be run.

    Examples:

        >>> import dataclasses
        >>> import norma
        >>>
        >>> @dataclasses.dataclass
        ... class Foo:
        ...     bar: str
        ...
        >>>
        >>> class FooService(norma.QueryService):
        ...     model = Foo
        ...
        ...     @norma.middleware("get", "get_cursor")
        ...     async def finalize_gets(
        ...         self,
        ...         query: protos.QueryMethodProtocol,
        ...         *args,
        ...         connection: protos.ConnectionT = None,
        ...         **kwargs
        ...    ) -> Foo:
        ...         print(f"Calling {query.__name__!r}.")
        ...         ...  # do stuff to foo
        ...         return query(self, *args, connection=connection, **kwargs)
        ...

    """

    def _middleware_wrapper(func: protos.MiddelwareMethodProtocol):
        return _middlware(*queries, func=func)

    return _middleware_wrapper


class _middlware:
    """A custom descriptor for attaching middleware to a query method.."""

    __slots__ = ("queries", "func")

    def __init__(self, *queries: str, func: protos.MiddelwareMethodProtocol):
        self.queries = queries
        self.func = func

    def __set_name__(self, owner: protos.ServiceProtocolT, name: str):

        self._bind_middleware(owner)
        setattr(owner, name, self.func)

    def _bind_middleware(self, owner: protos.ServiceProtocolT):
        mw = self.func
        for qname in self.queries:
            query: protos.QueryMethodProtocol = getattr(owner, qname)

            @functools.wraps(query)  # type: ignore
            def _wrap_query(
                self: protos.ServiceProtocolT, *args, __mw=mw, __q=query, **kwargs
            ):
                return __mw(self, __q, *args, **kwargs)

            setattr(owner, qname, _wrap_query)


CoerceableWrapperT = Callable[[protos.QueryMethodProtocol], protos.ModelMethodProtocol]
BulkCoerceableWrapperT = Callable[[protos.RawBulkProtocolT], protos.BulkModelProtocolT]


@overload
def coerceable(func: protos.RawProtocolT) -> protos.ModelProtocolT:
    ...


@overload
def coerceable(func: protos.RawPersistProtocolT) -> protos.ModelPersistProtocolT:
    ...


@overload
def coerceable(
    func: protos.RawProtocolT, *, bulk: Literal[False] = False
) -> protos.ModelProtocolT:
    ...


@overload
def coerceable(
    func: protos.RawPersistProtocolT, *, bulk: Literal[False] = False
) -> protos.ModelPersistProtocolT:
    ...


@overload
def coerceable(*, bulk: Literal[False] = False) -> CoerceableWrapperT:
    ...


@overload
def coerceable(
    func: protos.RawBulkProtocolT, *, bulk: Literal[True]
) -> protos.BulkModelProtocolT:
    ...


@overload
def coerceable(
    func: protos.RawBulkPersistProtocolT, *, bulk: Literal[True]
) -> protos.BulkModelPersistProtocolT:
    ...


@overload
def coerceable(*, bulk: Literal[True]) -> BulkCoerceableWrapperT:
    ...


def coerceable(func=None, *, bulk=False):
    """A helper which will automatically coerce a query result to a model."""
    if bulk:
        bulk_func = cast(Optional[protos.RawBulkProtocolT], func)
        return (
            _maybe_coerce_bulk_result(bulk_func)
            if bulk_func
            else _maybe_coerce_bulk_result
        )

    row_func = cast(Optional[protos.RawProtocolT], func)
    return _maybe_coerce_result(row_func) if row_func else _maybe_coerce_result


def _maybe_coerce_bulk_result(f: protos.RawBulkProtocolT) -> protos.BulkModelProtocolT:
    @functools.wraps(f)
    async def _maybe_coerce_bulk_result_wrapper(
        self: protos.ServiceProtocolT[protos.ModelT],
        *args,
        connection: protos.ConnectionT = None,
        coerce: bool = True,
        **kwargs,
    ) -> Union[Iterable[protos.ModelT], Iterable[protos.RawT]]:
        res: Iterable[protos.RawT] = await f(
            self, *args, connection=connection, **kwargs
        )
        if res and coerce:
            return self.bulk_protocol.transmute(({**r} for r in res))  # type: ignore
        return res

    return cast(protos.BulkModelProtocolT, _maybe_coerce_bulk_result_wrapper)


def _maybe_coerce_result(f: protos.RawProtocolT) -> protos.ModelProtocolT:
    @functools.wraps(f)  # type: ignore
    async def _maybe_coerce_result_wrapper(
        self: protos.ServiceProtocolT[protos.ModelT],
        *args,
        connection: protos.ConnectionT = None,
        coerce: bool = True,
        **kwargs,
    ) -> Union[protos.ModelT, protos.RawT, None]:
        res = await f(self, *args, connection=connection, **kwargs)
        if res and coerce:
            return self.protocol.transmute({**res})
        return res

    return cast(protos.ModelProtocolT, _maybe_coerce_result_wrapper)


def retry(
    func: Callable[..., Awaitable] = None,
    /,
    *errors: Type[BaseException],
    retries: int = 10,
    delay: float = 0.1,
):
    """Automatically retry a database operation on a transient error.

    "Transient" errors are configured by the connector protocol.
    """

    def _retry_impl(
        func_: Callable[..., Awaitable],
        *,
        _retries=retries,
        _errors=errors,
    ):
        _logger = logging.getLogger(__name__)

        @functools.wraps(func_)
        async def _retry(self: protos.ServiceProtocolT, *args, **kwargs):
            try:
                return await func_(self, *args, **kwargs)
            except (*_errors, *self.connector.TRANSIENT) as e:
                _logger.info(
                    "Got a watched error. Entering retry loop.",
                    error=e.__class__.__name__,
                    exception=str(e),
                )
                tries = 0
                while tries < _retries:
                    tries += 1
                    await asyncio.sleep(delay)
                    try:
                        return await func_(*args, **kwargs)
                    except _errors:
                        _logger.warning("Failed on retry.", retry=tries)
                _logger.error("Couldn't recover on retries. Re-raising original error.")
                raise e

        return _retry

    return _retry_impl(func) if func else _retry_impl


def retry_cursor(
    func: Callable[..., AsyncContextManager] = None,
    /,
    *errors: Type[BaseException],
    retries: int = 10,
    delay: float = 0.1,
):
    """Automatically retry a database operation on a transient error.

    "Transient" errors are configured by the connector protocol.
    """

    def _retry_impl(
        func_: Callable[..., AsyncContextManager],
        *,
        _retries=retries,
        _errors=errors,
    ):
        _logger = logging.getLogger(__name__)

        @contextlib.asynccontextmanager
        @functools.wraps(func_)
        async def _retry_cursor(self: protos.ServiceProtocolT, *args, **kwargs):
            try:
                async with func_(self, *args, **kwargs) as cm:
                    yield cm
            except (*_errors, *self.connector.TRANSIENT) as e:
                _logger.info(
                    "Got a watched error. Entering retry loop.",
                    error=e.__class__.__name__,
                    exception=str(e),
                )
                tries = 0
                while tries < _retries:
                    tries += 1
                    await asyncio.sleep(delay)
                    try:
                        async with func_(self, *args, **kwargs) as cm:
                            yield cm
                    except _errors:
                        _logger.warning("Failed on retry.", retry=tries)
                _logger.error("Couldn't recover on retries. Re-raising original error.")
                raise e

        return _retry_cursor

    return _retry_impl(func) if func else _retry_impl


def get_connector_protocol(
    driver: drivers.SupportedDriversT, **connect_kwargs
) -> protos.ConnectorProtocol:
    """Fetch a ConnectorProtocol and instantiate with the given kwargs."""
    if driver not in _DRIVER_TO_CONNECTOR:
        raise ValueError(
            f"Supported drivers are: {(*_DRIVER_TO_CONNECTOR,)}. Got: {driver!r}"
        )
    return _DRIVER_TO_CONNECTOR[driver](**connect_kwargs)  # type: ignore


_DRIVER_TO_CONNECTOR: Mapping[
    drivers.SupportedDriversT, Type[protos.ConnectorProtocol]
] = {
    "asyncpg": drivers.AsyncPGConnector,
    "aiosqlite": drivers.AIOSQLiteConnector,
}


def isbulk(func: QueryFn) -> bool:
    """Whether this query function may return multiple records."""
    return func.operation in _BULK_QUERIES


def isscalar(func: QueryFn) -> bool:
    """Whether the return value of this query function is not represented by the model."""
    return func.operation in _SCALAR_QUERIES


def ismutate(func: QueryFn) -> bool:
    """Whether this function results in a mutation of data."""
    return func.operation in _MUTATE_QUERIES


def ispersist(func: QueryFn) -> bool:
    """Whether this query function represents a creation or update of data."""
    sql = func.sql.lower()
    return "insert" in sql or "update" in sql


_SCALAR_QUERIES = {
    SQLOperationType.SELECT_VALUE,
    SQLOperationType.INSERT_UPDATE_DELETE,
    SQLOperationType.INSERT_UPDATE_DELETE_MANY,
    SQLOperationType.SCRIPT,
}
_BULK_QUERIES = {SQLOperationType.SELECT, SQLOperationType.INSERT_UPDATE_DELETE_MANY}
_MUTATE_QUERIES = {
    SQLOperationType.INSERT_UPDATE_DELETE,
    SQLOperationType.INSERT_UPDATE_DELETE_MANY,
    SQLOperationType.SCRIPT,
}
