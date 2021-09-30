import functools

from . import types

__all__ = ("middleware",)


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
        >>> import norma.aio
        >>>
        >>> @dataclasses.dataclass
        ... class Foo:
        ...     bar: str
        ...
        >>>
        >>> class FooService(norma.aio.QueryService[Foo]):
        ...
        ...     @norma.middleware("get", "get_cursor")
        ...     async def intercept_gets(
        ...         self,
        ...         query: types.QueryMethodProtocolT,
        ...         *args,
        ...         connection: types.ConnectionT = None,
        ...         **kwargs
        ...    ) -> Foo:
        ...         print(f"Calling {query.__name__!r}.")
        ...         ...  # do stuff to foo
        ...         return await query(self, *args, connection=connection, **kwargs)
        ...

    """

    def _middleware_wrapper(func: types.MiddelwareMethodProtocolT):
        return _middleware(*queries, func=func)

    return _middleware_wrapper


class _middleware:
    """A custom descriptor for attaching middleware to a query method.."""

    __slots__ = ("queries", "func")

    def __init__(self, *queries: str, func: types.MiddelwareMethodProtocolT):
        self.queries = queries
        self.func = func

    def __set_name__(self, owner: types.ServiceProtocolT, name: str):
        self._bind_middleware(owner)
        setattr(owner, name, self.func)

    def _bind_middleware(self, owner: types.ServiceProtocolT):
        mw = self.func
        for qname in self.queries:
            query: types.QueryMethodProtocolT = getattr(owner, qname)

            @functools.wraps(query)  # type: ignore
            def _wrap_query(
                self: types.ServiceProtocolT, *args, __mw=mw, __q=query, **kwargs
            ):
                return __mw(self, __q, *args, **kwargs)

            setattr(owner, qname, _wrap_query)
