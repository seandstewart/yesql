from __future__ import annotations

import asyncio
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
)

from aiosql.types import QueryFn, SQLOperationType

from norma import protos, drivers


QueryFunctionT = Union[protos.QueryMethodProtocolT, protos.QueryMethodBulkProtocolT]
CoerceableT = Union[protos.CoerceableProtocolT, protos.BulkCoerceableProtocolT]
CoerceableWrapperT = Callable[
    [protos.QueryMethodProtocolT[protos.ModelT]],
    protos.CoerceableProtocolT[protos.ModelT],
]
BulkCoerceableWrapperT = Callable[
    [protos.QueryMethodBulkProtocolT[protos.ModelT]],
    protos.BulkCoerceableProtocolT[protos.ModelT],
]


@overload
def coerceable(
    func: protos.QueryMethodProtocolT[protos.ModelT], *, bulk: bool = False
) -> protos.CoerceableProtocolT[protos.ModelT]:
    ...


@overload
def coerceable(*, bulk: Literal[False] = False) -> CoerceableWrapperT:
    ...


@overload
def coerceable(
    func: protos.QueryMethodBulkProtocolT[protos.ModelT], *, bulk: Literal[True]
) -> protos.BulkCoerceableProtocolT[protos.ModelT]:
    ...


@overload
def coerceable(*, bulk: Literal[True]) -> BulkCoerceableWrapperT:
    ...


def coerceable(func: QueryFunctionT = None, *, bulk: bool = False):
    """A helper which will automatically coerce a protos.RawT to a model."""
    if bulk:
        bulk_func = cast(Optional[protos.QueryMethodBulkProtocolT], func)
        return (
            _maybe_coerce_bulk_result(bulk_func)
            if bulk_func
            else _maybe_coerce_bulk_result
        )

    row_func = cast(Optional[protos.QueryMethodProtocolT], func)
    return _maybe_coerce_result(row_func) if row_func else _maybe_coerce_result


def retry(
    func: Callable[..., Awaitable] = None,
    /,
    *errors: Type[BaseException],
    retries: int = 10,
    delay: float = 0.1,
):
    """Automatically retry a database operation on a transient error.

    Default errors are:
        - asyncpg.DeadlockDetectedError
        - asyncpg.TooManyConnectionsError
        - asyncpg.PostgresConnectionError
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


def _maybe_coerce_bulk_result(
    f: protos.QueryMethodBulkProtocolT[protos.ModelT],
) -> protos.BulkCoerceableProtocolT[protos.ModelT]:
    @functools.wraps(f)
    async def _maybe_coerce_bulk_result_wrapper(
        self: protos.ServiceProtocolT[protos.ModelT],
        *args,
        connection: protos.ConnectionT = None,
        coerce: bool = True,
        **kwargs,
    ) -> Union[Iterable[protos.ModelT], Iterable[protos.RawT]]:
        res: Iterable[protos.RawT] = await f(
            self, *args, coerce=coerce, connection=connection, **kwargs
        )
        if res and coerce:
            return self.bulk_protocol.transmute(({**r} for r in res))  # type: ignore
        return res

    return cast(protos.BulkCoerceableProtocolT, _maybe_coerce_bulk_result_wrapper)


def _maybe_coerce_result(
    f: protos.QueryMethodProtocolT[protos.ModelT],
) -> protos.CoerceableProtocolT[protos.ModelT]:
    @functools.wraps(f)  # type: ignore
    async def _maybe_coerce_result_wrapper(
        self: protos.ServiceProtocolT[protos.ModelT],
        *args,
        connection: protos.ConnectionT = None,
        coerce: bool = True,
        **kwargs,
    ) -> Union[protos.ModelT, protos.RawT, None]:
        res = await f(self, *args, coerce=coerce, connection=connection, **kwargs)
        if res and coerce:
            return self.protocol.transmute({**res})
        return res

    return cast(protos.CoerceableProtocolT, _maybe_coerce_result_wrapper)


def get_connector_protocol(
    driver: drivers.SupportedDriversT, **connect_kwargs
) -> protos.ConnectorProtocol:
    if driver not in _DRIVER_TO_CONNECTOR:
        raise ValueError(
            f"Supported drivers are: {(*_DRIVER_TO_CONNECTOR,)}. Got: {driver!r}"
        )
    return _DRIVER_TO_CONNECTOR[driver](**connect_kwargs)  # type: ignore


_DRIVER_TO_CONNECTOR: Mapping[
    drivers.SupportedDriversT, Type[protos.ConnectorProtocol]
] = {
    "asyncpg": drivers.asyncpg.AsyncPGConnector,
    "aiosqlite": drivers.aiosqlite.AIOSQLiteConnector,
}


def isbulk(func: QueryFn):
    """Whether this query function may return multiple records."""
    return func.operation in _BULK_QUERIES


def isscalar(func: QueryFn):
    """Whether the return value of this query function is not represented by the model."""
    return func.operation in _SCALAR_QUERIES


def ispersist(func: QueryFn):
    """Whether this query function represents a creation or update of data."""
    sql = func.sql.lower()
    return "insert" in sql or "update" in sql


_SCALAR_QUERIES = {
    SQLOperationType.SELECT_VALUE,
    SQLOperationType.INSERT_UPDATE_DELETE,
    SQLOperationType.INSERT_UPDATE_DELETE_MANY,
    SQLOperationType.SCRIPT,
}
_BULK_QUERIES = {SQLOperationType.SELECT}
