from . import types

__all__ = ("middleware",)


def middleware(*names: str):
    """Flag to the service that this method is a 'middleware' for the target query.

    A 'middleware' method allows for pre- and post-processing of for the assigned query
    methods.

    Notes:
        Middlewares should always be a coroutine function.

    Args:
        *names: str
            The queries for which this middelwares should be run.

    Examples:

        >>> import dataclasses
        >>> import pathlib
        >>> import norma
        >>> from tests.unit.queries import QUERIES
        >>>
        >>>
        >>> @dataclasses.dataclass
        ... class Foo:
        ...     bar: str
        ...
        >>>
        >>> class FooService(norma.AsyncQueryService[Foo]):
        ...
        ...     class metadata(norma.QueryMetadata):
        ...         __querylib__ = QUERIES
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
        func.__intercepts__ = names
        return func

    return _middleware_wrapper
