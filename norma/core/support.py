from __future__ import annotations

import asyncio
import contextlib
import importlib
import inspect
import functools
import logging
import time
from types import ModuleType
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
    TypeVar,
    NoReturn,
    ContextManager,
    Any,
)

import orjson
import typic

from . import types, drivers

__all__ = (
    "coerceable",
    "get_connector_protocol",
    "retry",
    "retry_cursor",
    "dumps",
    "loads",
)

_T = TypeVar("_T")
_MaybeAwaitable = [Union[_T, Awaitable[_T]]]
_SyncCoerceableWrapperT = Callable[
    [types.QueryMethodProtocolT[_T]], types.ModelMethodProtocolT[_T]
]
_AsyncCoerceableWrapperT = Callable[
    [types.QueryMethodProtocolT[Awaitable[_T]]],
    types.ModelMethodProtocolT[Awaitable[_T]],
]
CoerceableWrapperT = Union[_SyncCoerceableWrapperT, _AsyncCoerceableWrapperT]
_SyncBulkCoerceableWrapperT = Callable[
    [types.ScalarBulkMethodProtocolT[_T]], types.ModelBulkMethodProtocolT[_T]
]
_AsyncBulkCoerceableWrapperT = Callable[
    [types.ScalarBulkMethodProtocolT[Awaitable[_T]]],
    types.ModelBulkMethodProtocolT[Awaitable[_T]],
]

BulkCoerceableWrapperT = Union[
    _SyncBulkCoerceableWrapperT, _AsyncBulkCoerceableWrapperT
]


@overload
def coerceable(
    func: types.ScalarPersistProtocolT[Awaitable[_T]],
) -> types.ModelPersistProtocolT[Awaitable[_T]]:
    ...


@overload
def coerceable(
    func: types.ScalarPersistProtocolT[Awaitable[_T]], *, bulk: Literal[False]
) -> types.ModelPersistProtocolT[Awaitable[_T]]:
    ...


@overload
def coerceable(
    func: types.ScalarBulkPersistProtocolT[NoReturn], *, bulk: Literal[True]
) -> types.ModelBulkPersistProtocolT[NoReturn]:
    ...


@overload
def coerceable(
    func: types.ScalarBulkMethodProtocolT[Awaitable[_T]], *, bulk: Literal[True]
) -> types.ModelBulkMethodProtocolT[Awaitable[_T]]:
    ...


@overload
def coerceable(
    func: types.ScalarBulkPersistProtocolT[Awaitable[NoReturn]], *, bulk: Literal[True]
) -> types.ModelBulkPersistProtocolT[Awaitable[NoReturn]]:
    ...


@overload
def coerceable(
    func: types.ScalarPersistProtocolT[_T],
) -> types.ModelPersistProtocolT[_T]:
    ...


@overload
def coerceable(
    func: types.ScalarPersistProtocolT[_T], *, bulk: Literal[False]
) -> types.ModelPersistProtocolT[_T]:
    ...


@overload
def coerceable(
    func: types.QueryMethodProtocolT[Awaitable[_T]],
) -> types.ModelMethodProtocolT[Awaitable[_T]]:
    ...


@overload
def coerceable(
    func: types.QueryMethodProtocolT[Awaitable[_T]], *, bulk: Literal[False]
) -> types.ModelMethodProtocolT[Awaitable[_T]]:
    ...


@overload
def coerceable(
    func: types.QueryMethodProtocolT[_T], *, bulk: Literal[False]
) -> types.ModelMethodProtocolT[_T]:
    ...


@overload
def coerceable(func: types.QueryMethodProtocolT[_T]) -> types.ModelMethodProtocolT[_T]:
    ...


@overload
def coerceable(*, bulk: Literal[False]) -> CoerceableWrapperT:
    ...


@overload
def coerceable(
    func: types.ScalarBulkMethodProtocolT[_T], *, bulk: Literal[True]
) -> types.ModelBulkMethodProtocolT[_T]:
    ...


@overload
def coerceable(*, bulk: Literal[True]) -> BulkCoerceableWrapperT:
    ...


def coerceable(func=None, *, bulk=False):
    """A helper which will automatically coerce a query result to a model."""
    if bulk:
        bulk_func = cast(Optional[types.ScalarBulkPersistProtocolT], func)
        return (
            _maybe_coerce_bulk_result(bulk_func)
            if bulk_func
            else _maybe_coerce_bulk_result
        )

    row_func = cast(Optional[types.QueryMethodProtocolT], func)
    return _maybe_coerce_result(row_func) if row_func else _maybe_coerce_result


def _maybe_coerce_bulk_result(
    f: types.ScalarBulkMethodProtocolT,
) -> types.ModelBulkMethodProtocolT:
    if inspect.iscoroutinefunction(inspect.unwrap(f)):
        af = cast(Callable[..., Awaitable], f)

        @functools.wraps(af)
        async def _maybe_coerce_bulk_result_wrapper(
            self: types.ServiceProtocolT,
            *args,
            connection: types.ConnectionT = None,
            coerce: bool = True,
            **kwargs,
        ) -> Union[Iterable[types.ModelT], Iterable[types.ScalarT]]:
            res: Iterable[types.ScalarT] = await af(
                self, *args, connection=connection, **kwargs
            )
            if res and coerce:
                return self.bulk_protocol.transmute(res)  # type: ignore
            return res

    else:

        @functools.wraps(f)
        def _maybe_coerce_bulk_result_wrapper(
            self: types.ServiceProtocolT,
            *args,
            connection: types.ConnectionT = None,
            coerce: bool = True,
            **kwargs,
        ) -> Union[Iterable[types.ModelT], Iterable[types.ScalarT]]:
            res: Iterable[types.ScalarT] = f(
                self, *args, connection=connection, **kwargs
            )
            if res and coerce:
                return self.bulk_protocol.transmute(res)  # type: ignore
            return res

    return cast(types.ModelMethodProtocolT, _maybe_coerce_bulk_result_wrapper)


def _maybe_coerce_result(f: types.QueryMethodProtocolT) -> types.ModelMethodProtocolT:
    if inspect.iscoroutinefunction(inspect.unwrap(f)):

        @functools.wraps(f)  # type: ignore
        async def _maybe_coerce_result_wrapper(
            self: types.ServiceProtocolT,
            *args,
            connection: types.ConnectionT = None,
            coerce: bool = True,
            **kwargs,
        ) -> Union[types.ModelT, types.ScalarT, None]:
            res = await f(self, *args, connection=connection, **kwargs)
            if res and coerce:
                return self.protocol.transmute(res)
            return res

    else:

        @functools.wraps(f)  # type: ignore
        def _maybe_coerce_result_wrapper(
            self: types.ServiceProtocolT,
            *args,
            connection: types.ConnectionT = None,
            coerce: bool = True,
            **kwargs,
        ) -> Union[types.ModelT, types.ScalarT, None]:
            res = f(self, *args, connection=connection, **kwargs)
            if res and coerce:
                return self.protocol.transmute(res)
            return res

    return cast(types.ModelMethodProtocolT, _maybe_coerce_result_wrapper)


def retry(
    func: Union[Callable[..., Awaitable[_T]], Callable[..., _T]] = None,
    /,
    *errors: Type[BaseException],
    retries: int = 10,
    delay: float = 0.1,
):
    """Automatically retry a database operation on a transient error.

    "Transient" errors are configured by the connector protocol.
    """

    def _retry_impl(
        func_: Union[Callable[..., Awaitable[_T]], Callable[..., _T]],
        *,
        _retries=retries,
        _errors=errors,
    ):
        _logger = logging.getLogger(getattr(func_, "__module__", __name__))
        ufunc = inspect.unwrap(func_)
        if inspect.iscoroutinefunction(ufunc):
            afunc = cast(Callable[..., Awaitable[_T]], func_)

            @functools.wraps(afunc)
            async def _retry(self: types.ServiceProtocolT, *args, **kwargs):
                try:
                    return await afunc(self, *args, **kwargs)
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
                            return await afunc(self, *args, **kwargs)
                        except _errors:
                            _logger.warning("Failed on retry.", retry=tries)
                    _logger.error(
                        "Couldn't recover on retries. Re-raising original error."
                    )
                    raise e

        else:

            @functools.wraps(func_)
            def _retry(self: types.ServiceProtocolT, *args, **kwargs):
                try:
                    return func_(self, *args, **kwargs)
                except (*_errors, *self.connector.TRANSIENT) as e:
                    _logger.info(
                        "Got a watched error. Entering retry loop. "
                        "%(error): %(exception)",
                        error=e.__class__.__name__,
                        exception=str(e),
                    )
                    tries = 0
                    while tries < _retries:
                        tries += 1
                        time.sleep(delay)
                        try:
                            return func_(self, *args, **kwargs)
                        except _errors:
                            _logger.warning("Failed on retry=%(retry)s.", retry=tries)
                    _logger.error(
                        "Couldn't recover on retries. Re-raising original error."
                    )
                    raise e

        return _retry

    return _retry_impl(func) if func else _retry_impl


def retry_cursor(
    func: Callable[..., Union[AsyncContextManager, ContextManager]] = None,
    /,
    *errors: Type[BaseException],
    retries: int = 10,
    delay: float = 0.1,
):
    """Automatically retry a database operation on a transient error.

    "Transient" errors are configured by the connector protocol.
    """

    def _retry_impl(
        func_: Callable[..., Union[AsyncContextManager, ContextManager]],
        *,
        _retries=retries,
        _errors=errors,
    ):
        _logger = logging.getLogger(__name__)
        ufunc_ = inspect.unwrap(func_)
        if inspect.isasyncgenfunction(ufunc_):
            afunc = cast(Callable[..., AsyncContextManager], func_)

            @contextlib.asynccontextmanager
            @functools.wraps(afunc)
            async def _retry_cursor(self: types.ServiceProtocolT, *args, **kwargs):
                try:
                    async with afunc(self, *args, **kwargs) as cm:
                        yield cm
                except (*_errors, *self.connector.TRANSIENT) as e:
                    _logger.info(
                        "Got a watched error. Entering retry loop. "
                        "%(error): %(exception)",
                        error=e.__class__.__name__,
                        exception=str(e),
                    )
                    tries = 0
                    while tries < _retries:
                        tries += 1
                        await asyncio.sleep(delay)
                        try:
                            async with afunc(self, *args, **kwargs) as cm:
                                yield cm
                        except _errors:
                            _logger.warning("Failed on retry=%(retry)s.", retry=tries)
                    _logger.error(
                        "Couldn't recover on retries. Re-raising original error."
                    )
                    raise e

        else:
            sfunc = cast(Callable[..., ContextManager], func_)

            @contextlib.contextmanager
            @functools.wraps(sfunc)
            def _retry_cursor(self: types.ServiceProtocolT, *args, **kwargs):
                try:
                    with sfunc(self, *args, **kwargs) as cm:
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
                        time.sleep(delay)
                        try:
                            with sfunc(self, *args, **kwargs) as cm:
                                yield cm
                        except _errors:
                            _logger.warning("Failed on retry.", retry=tries)
                    _logger.error(
                        "Couldn't recover on retries. Re-raising original error."
                    )
                    raise e

        return _retry_cursor

    return _retry_impl(func) if func else _retry_impl


def get_connector_protocol(
    driver: drivers.SupportedDriversT, **connect_kwargs
) -> types.AnyConnectorProtocolT:
    """Fetch a ConnectorProtocol and instantiate with the given kwargs."""
    if driver not in _DRIVER_TO_CONNECTOR:
        raise ValueError(
            f"Supported drivers are: {(*_DRIVER_TO_CONNECTOR,)}. Got: {driver!r}"
        )
    modname, cname = _DRIVER_TO_CONNECTOR[driver].rsplit(".", maxsplit=1)
    mod = _try_import(modname, driver=driver)
    ctype: Type[types.AnyConnectorProtocolT] = getattr(mod, cname)
    return ctype(**connect_kwargs)  # type: ignore


def get_cursor_proxy(driver: drivers.SupportedDriversT) -> Type:
    """Get a proxy type which ensures compliance to the defined CursorProtocol."""
    if driver not in _DRIVER_TO_CURSOR_PROXY:
        return lambda c: c  # type: ignore
    modname, cname = _DRIVER_TO_CURSOR_PROXY[driver].rsplit(".", maxsplit=1)
    mod = _try_import(modname, driver=driver)
    ctype: Type[types.AnyConnectorProtocolT] = getattr(mod, cname)
    return ctype


def _try_import(modname: str, *, driver: str) -> ModuleType:
    try:
        return importlib.import_module(name=modname)
    except (ImportError, ModuleNotFoundError) as e:
        raise RuntimeError(
            f"Couldn't import driver. Is {driver!r} installed in your environment?"
        ) from e


_DRIVER_TO_CONNECTOR: _DriverMappingT = {
    "aiosqlite": "norma.core.drivers.aio.sqlite.AIOSQLiteConnector",
    "asyncpg": "norma.core.drivers.aio.postgres.AsyncPGConnector",
    "psycopg": "norma.core.drivers.sio.postgres.PsycoPGConnector",
    "sqlite": "norma.core.drivers.sio.sqlite.SQLiteConnector",
}

_DRIVER_TO_CURSOR_PROXY: _DriverMappingT = {
    "aiosqlite": "norma.core.drivers.aio.sqlite._AIOSQLiteCursorProxy",
    "psycopg": "norma.core.drivers.sio.postgres._PsycoPGCursorProxy",
    "sqlite": "norma.core.drivers.sio.sqlite._SQLite3CursorProxy",
}

_DriverMappingT = Mapping[drivers.SupportedDriversT, str]


def dumps(o: Any) -> str:
    """Encode any object to JSON."""

    # orjson encodes to binary, but libpq (the c bindings for postgres)
    # can't write binary data to JSONB columns.
    # https://github.com/lib/pq/issues/528
    # This is still orders of magnitude faster than any other lib.
    return orjson.dumps(o, default=typic.tojson).decode()


loads = orjson.loads
