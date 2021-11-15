from __future__ import annotations

import asyncio
import dataclasses
import importlib
import inspect
import functools
import time
import typing
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
    TYPE_CHECKING,
)

import orjson
import typic

from . import types

if TYPE_CHECKING:
    from norma import drivers

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
    if _isasync(f):
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
    if _isasync(f):

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
        if _isasync(func_):
            afunc = cast(Callable[..., Awaitable[_T]], func_)

            @functools.wraps(afunc)
            async def _retry(self: types.ServiceProtocolT, *args, **kwargs):
                errs = (*_errors, *self.connector.TRANSIENT)
                async with _AsyncRetryContext(
                    func=afunc,
                    args=args,
                    kwargs=kwargs,
                    errors=errs,
                    retries=_retries,
                    delay=delay,
                ) as result:
                    return result

        else:

            @functools.wraps(func_)
            def _retry(self: types.ServiceProtocolT, *args, **kwargs):
                errs = (*_errors, *self.connector.TRANSIENT)
                with _SyncRetryContext(
                    func=func_,
                    args=args,
                    kwargs=kwargs,
                    errors=errs,
                    retries=_retries,
                    delay=delay,
                ) as result:
                    return result

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
        context_cls = (
            _AsyncRetryCursorContext if _isasync(func_) else _SyncRetryCursorContext
        )

        @functools.wraps(func_)
        def _retry_cursor(self: types.ServiceProtocolT, *args, **kwargs):
            errs = (*_errors, *self.connector.TRANSIENT)
            return context_cls(
                func=func_,
                args=args,
                kwargs=kwargs,
                errors=errs,
                retries=retries,
                delay=delay,
            )

        return _retry_cursor

    return _retry_impl(func) if func else _retry_impl


@typic.slotted(dict=False, weakref=True)
@dataclasses.dataclass
class _RetryContext:
    func: Callable
    args: tuple
    kwargs: dict
    errors: tuple[Type[BaseException], ...]
    retries: int
    delay: float

    def _do_exec(self):
        return self.func(*self.args, **self.kwargs)


class _SyncRetryContext(_RetryContext):
    def __enter__(self):
        do, tries, retries, errors, delay = (
            self._do_exec,
            0,
            self.retries,
            self.errors,
            self.delay,
        )
        try:
            return do()
        except errors as e:
            time.sleep(delay)
            while tries < retries:
                tries += 1
                try:
                    return do()
                except errors:
                    time.sleep(delay)
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class _SyncRetryCursorContext(_SyncRetryContext):
    func: Callable[..., ContextManager]

    def _do_exec(self):
        call = self.func(*self.args, **self.kwargs)
        return call.__enter__()


class _AsyncRetryContext(_RetryContext):
    async def __aenter__(self):
        do, tries, retries, errors, delay = (
            self._do_exec,
            0,
            self.retries,
            self.errors,
            self.delay,
        )
        try:
            return await do()
        except errors as e:
            await asyncio.sleep(delay)
            while tries < retries:
                tries += 1
                try:
                    return await do()
                except errors:
                    await asyncio.sleep(delay)
            raise e

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class _AsyncRetryCursorContext(_AsyncRetryContext):
    def _do_exec(self):
        call = self.func(*self.args, **self.kwargs)
        return call.__aenter__()


def _isasync(f):
    unwrapped = inspect.unwrap(f)
    hints = typing.get_type_hints(unwrapped)
    returns = oreturns = hints.get("returns")
    if returns:
        oreturns = typing.get_origin(returns)
    return (
        inspect.iscoroutinefunction(unwrapped)
        or inspect.isasyncgenfunction(unwrapped)
        or typic.get_name(oreturns)
        in {
            "Awaitable",
            "AsyncIterable",
            "AsyncIterator",
            "AsyncGenerator",
            "Coroutine",
        }
    )


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


def get_cursor_proxy(driver: drivers.SupportedDriversT) -> Type[types.CursorProtocolT]:
    """Get a proxy type which ensures compliance to the defined CursorProtocol."""
    if driver not in _DRIVER_TO_CURSOR_PROXY:
        return lambda c: c  # type: ignore
    modname, cname = _DRIVER_TO_CURSOR_PROXY[driver].rsplit(".", maxsplit=1)
    mod = _try_import(modname, driver=driver)
    ctype: Type[types.CursorProtocolT] = getattr(mod, cname)
    return ctype


def _try_import(modname: str, *, driver: str) -> ModuleType:
    try:
        return importlib.import_module(name=modname)
    except (ImportError, ModuleNotFoundError) as e:
        raise RuntimeError(
            f"Couldn't import driver. Is {driver!r} installed in your environment?"
        ) from e


_DRIVER_TO_CONNECTOR: Mapping[drivers.SupportedDriversT, str] = {
    "aiosqlite": "norma.drivers.aio.sqlite.AIOSQLiteConnector",
    "asyncpg": "norma.drivers.aio.postgres.AsyncPGConnector",
    "psycopg": "norma.drivers.sio.postgres.PsycoPGConnector",
    "sqlite": "norma.drivers.sio.sqlite.SQLiteConnector",
}

_DRIVER_TO_CURSOR_PROXY: Mapping[drivers.SupportedDriversT, str] = {
    "aiosqlite": "norma.drivers.aio.sqlite._AIOSQLiteCursorProxy",
    "psycopg": "norma.drivers.sio.postgres._PsycoPGCursorProxy",
    "sqlite": "norma.drivers.sio.sqlite._SQLite3CursorProxy",
}


def dumps(o: Any) -> str:
    """Encode any object to JSON."""

    # orjson encodes to binary, but libpq (the c bindings for postgres)
    # can't write binary data to JSONB columns.
    # https://github.com/lib/pq/issues/528
    # This is still orders of magnitude faster than any other lib.
    return orjson.dumps(o, default=typic.tojson).decode()


loads = orjson.loads
