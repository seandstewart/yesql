from __future__ import annotations

import asyncio
import dataclasses
import functools
import inspect
import time
import typing
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    Awaitable,
    Callable,
    ContextManager,
    Type,
    TypeVar,
    Union,
    cast,
)

import orjson
import typic

if TYPE_CHECKING:
    from yesql.core import types


__all__ = (
    "retry",
    "retry_cursor",
    "dumps",
    "dumpsb",
    "loads",
)

_T = TypeVar("_T")


def retry(
    func: Callable[..., Awaitable[_T]] | Callable[..., _T] = None,
    /,
    *errors: type[BaseException],
    retries: int = 10,
    delay: float = 0.1,
    isaio: bool = False,
):
    """Automatically retry a database operation on a transient error.

    "Transient" errors are configured by the query executor.
    """

    def _retry_impl(
        func_: Callable[..., Awaitable[_T]] | Callable[..., _T],
        *,
        _retries: int = retries,
        _errors: tuple[type[BaseException], ...] = errors,
    ):
        _isaio = isaio or _isasync(func_)
        if _isaio:
            afunc = cast(Callable[..., Awaitable[_T]], func_)

            @functools.wraps(afunc)
            async def _retry(self: types.RepositoryProtocolT, *args, **kwargs):
                errs = (*_errors, *self.TRANSIENT)
                async with _AsyncRetryContext(
                    svc=self,
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
            def _retry(self: types.RepositoryProtocolT, *args, **kwargs):
                errs = (*_errors, *self.TRANSIENT)
                with _SyncRetryContext(
                    svc=self,
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
    func: Callable[..., AsyncContextManager] | Callable[..., ContextManager] = None,
    /,
    *errors: Type[BaseException],
    retries: int = 10,
    delay: float = 0.1,
    isaio: bool = False,
):
    """Automatically retry a database operation on a transient error.

    "Transient" errors are configured by the connector protocol.
    """

    def _retry_impl(
        func_: Callable[..., Union[AsyncContextManager, ContextManager]],
        *,
        _retries: int = retries,
        _errors: tuple[type[BaseException], ...] = errors,
    ):
        _isaio = isaio or _isasync(func_)
        context_cls = _AsyncRetryCursorContext if _isaio else _SyncRetryCursorContext

        @functools.wraps(func_)
        def _retry_cursor(self: types.RepositoryProtocolT, *args, **kwargs):
            errs = (*_errors, *self.TRANSIENT)
            return context_cls(
                svc=self,
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
    svc: types.RepositoryProtocolT
    func: Callable
    args: tuple
    kwargs: dict
    errors: tuple[Type[BaseException], ...]
    retries: int
    delay: float

    def _do_exec(self):
        return self.func(self.svc, *self.args, **self.kwargs)


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
    __slots__ = ("ctx",)

    func: Callable[..., ContextManager]

    def _do_exec(self):
        self.ctx = self.func(self.svc, *self.args, **self.kwargs)
        return self.ctx.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.ctx.__exit__(exc_type, exc_val, exc_tb)


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
    __slots__ = ("ctx",)

    def _do_exec(self):
        self.ctx = self.func(self.svc, *self.args, **self.kwargs)
        return self.ctx.__aenter__()

    def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.ctx.__aexit__(exc_type, exc_val, exc_tb)


def _isasync(f):
    unwrapped = inspect.unwrap(f)
    hints = typic.get_type_hints(unwrapped)
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


def dumpsb(o: Any) -> bytes:
    """Encode any object to a JSON byte-string."""
    return orjson.dumps(typic.primitive(o))


def dumps(o: Any) -> str:
    """Encode any object to a JSON string."""

    # orjson encodes to binary, but libpq (the c bindings for postgres)
    # can't write binary data to JSONB columns.
    # https://github.com/lib/pq/issues/528
    # This is still orders of magnitude faster than any other lib.
    return orjson.dumps(typic.primitive(o)).decode()


loads = orjson.loads
