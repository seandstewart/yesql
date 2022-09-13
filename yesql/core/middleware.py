try:
    from typing import TypeGuard  # type: ignore[attr-defined]
except (ImportError, ModuleNotFoundError):
    from typing_extensions import TypeGuard

from . import types

__all__ = ("middleware",)


def middleware(*names: str):
    """Flag to the service that this method is a 'middleware' for the target query.

    A 'middleware' method allows for pre- and post-processing of for the assigned query
    methods. Doing so matkes the flagged callable behave more like a staticmethod than
    a bound method.

    Notes:
        Middlewares should always be a coroutine function.

    Args:
        *names: str
            The queries for which this middelwares should be run.

    Examples:

        >>> from __future__ import annotations
        >>>
        >>> import dataclasses
        >>> import pathlib
        >>> import yesql
        >>> from tests.unit.queries import QUERIES
        >>>
        >>>
        >>> @dataclasses.dataclass
        ... class Foo:
        ...     bar: str
        ...
        >>>
        >>> class FooService(yesql.AsyncQueryRepository[Foo]):
        ...
        ...     class metadata(yesql.QueryMetadata):
        ...         __querylib__ = QUERIES
        ...
        ...     @yesql.middleware("get", "get_cursor")
        ...     async def intercept_gets(
        ...         statement: yesql.Statement,
        ...         *args,
        ...         connection: "types.ConnectionT | None" = None,
        ...         timeout: float = 10,
        ...         transaction: bool = True,
        ...         rollback: bool = False,
        ...         **kwargs
        ...    ) -> Foo:
        ...         print(f"Executing {statement.query!r}.")
        ...         ...  # do stuff to foo
        ...         return await statement.execute(
        ...             *args,
        ...             connection=connection,
        ...             timeout=timeout,
        ...             transaction=transaction,
        ...             rollback=rollback,
        ...             **kwargs,
        ...         )
        ...

    """

    def _middleware_wrapper(func: types.MiddlewareMethodProtocolT):
        func.__intercepts__ = names
        return func

    return _middleware_wrapper


def ismiddleware(o) -> TypeGuard[types.MiddlewareMethodProtocolT]:
    return callable(o) and hasattr(o, "__intercepts__")
