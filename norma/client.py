from __future__ import annotations

import contextlib
import inspect
import logging
import pathlib
from typing import (
    Iterable,
    Mapping,
    Union,
    Any,
    Callable,
    Literal,
    ClassVar,
    FrozenSet,
    Type,
    Optional,
    Generic,
    TypeVar,
    cast,
    overload,
)

import aiosql
import inflection
import typic
from aiosql.types import QueryFn

from norma import protos, support, drivers

logger = logging.getLogger(__name__)


ExplainFormatT = Literal["json", "yaml", "xml"]

_MT = TypeVar("_MT")
_RT = TypeVar("_RT")


class CoercingCursor(protos.CursorProtocolT[_MT]):
    """A shim around a cursor which will automatically convert data to the model."""

    __slots__ = ("service", "cursor")

    def __init__(
        self,
        service: protos.ServiceProtocolT[_MT],
        cursor: protos.CursorProtocolT,
    ):
        self.service = service
        self.cursor = cursor

    def __getattr__(self, item: str):
        return self.cursor.__getattribute__(item)

    async def forward(self, n: int, *args, timeout: float = None, **kwargs):
        return await self.cursor.forward(n, *args, timeout=timeout, **kwargs)

    async def fetch(
        self, n: int, *args, timeout: float = None, **kwargs
    ) -> Iterable[_MT]:
        page = await self.cursor.fetch(n, *args, timeout=timeout, **kwargs)
        return page and self.service.bulk_protocol(({**r} for r in page))

    async def fetchrow(self, *args, timeout: float = None, **kwargs) -> Optional[_MT]:
        row = await self.cursor.fetchrow(*args, timeout=timeout, **kwargs)
        return row and self.service.protocol({**row})


class Metadata(protos.MetadataT):
    __slots__ = ()
    __driver__: ClassVar[drivers.SupportedDriversT] = "asyncpg"
    __exclude_fields__: ClassVar[FrozenSet[str]] = frozenset(
        ("id", "created_at", "updated_at")
    )
    __scalar_queries__: ClassVar[FrozenSet[str]] = frozenset(())
    __querylib__: ClassVar[Union[str, pathlib.Path]]


class QueryService(Generic[_MT]):
    """The base class for a 'service'.

    A 'service' is responsible for querying a specific table.
    It also is semi-aware of in-memory dataclass representing the table data.

    By default, a service will coerce the query result to the correct model.
    """

    # User-defined class attributes
    model: ClassVar[Type[_MT]] = Any  # type: ignore
    metadata: ClassVar[Type[protos.MetadataT]] = Metadata  # type: ignore
    # Generated attributes
    protocol: ClassVar[typic.SerdeProtocol[_MT]]
    bulk_protocol: ClassVar[typic.SerdeProtocol[Iterable[_MT]]]
    queries: ClassVar[aiosql.aiosql.Queries]

    __slots__ = ("connector",)

    def __init__(
        self,
        *,
        connector: protos.ConnectorProtocol = None,
        **connect_kwargs,
    ):
        self.connector: protos.ConnectorProtocol = (
            connector
            or support.get_connector_protocol(
                self.metadata.__driver__, **connect_kwargs
            )
        )

    def __init_subclass__(cls, **kwargs):
        for pcls in inspect.getmro(cls)[1:]:  # the first entry is always `cls`
            if not issubclass(pcls, QueryService):
                continue
            cls.metadata.__exclude_fields__ |= pcls.metadata.__exclude_fields__
            cls.metadata.__scalar_queries__ |= pcls.metadata.__scalar_queries__

        if not hasattr(cls.metadata, "__tablename__"):
            cls.metadata.__tablename__ = cls._get_table_name()

        cls.queries = cls._get_query_library()
        cls.protocol = typic.protocol(cls.model, is_optional=True)
        cls.bulk_protocol = typic.protocol(Iterable[cls.model])
        cls._bootstrap_user_queries()
        super().__init_subclass__()

    @support.retry
    async def count(
        self,
        query: Union[str, Callable],
        *args,
        connection: protos.ConnectionT = None,
        **kwargs,
    ) -> int:
        """Get the number of rows returned by this query.

        Args:
            query:
                Either the query function, or the name of the query in your library
                which you wish to analyze.
            *args:
                Any positional arguments which the query requires.
            connection: optional
                A raw DBAPI connection object.
            **kwargs:
                Any keyword-arguments you'll pass on to the query.

        Returns:
            The number of rows.
        """
        name = query if isinstance(query, str) else query.__name__
        queryfn: aiosql.types.QueryFn = getattr(self.queries, name)
        sql = f"SELECT count(*) FROM ({queryfn.sql.rstrip(';')}) AS q;"
        async with self.connector.connection(c=connection) as c:
            return await self.queries.driver_adapter.select_value(
                c,
                query_name=name,
                sql=sql,
                parameters=kwargs or args,
            )

    @support.retry
    async def explain(
        self,
        query: Union[str, Callable],
        *args,
        analyze: bool = True,
        connection: protos.ConnectionT = None,
        format: Optional[ExplainFormatT] = "json",
        **kwargs,
    ) -> Union[protos.RawT, str]:
        """Get profiling information from the database about your query.

        EXPLAIN is a useful tool to debug how the RDBMS's query optimizer will execute
        your query in the database so that you can tune either it or the schema for
        your use-case.

        Notes:
            We run our EXPLAIN under a transaction which is automatically rolled back,
            so this operation is considered "safe" to use with queries which would
            result in mutation.

            The exact command run is determined by the ConnectorProtocol's
            `get_explain_command`, which will return a compliant command for the
            selected dialect. Consult your dialect's documentation for more information.

        Args:
            query:
                Either the query function, or the name of the query in your library
                which you wish to analyze.
            *args:
                Any positional arguments which the query requires.
            analyze: defaults True
                If true and supported by your dialect, run `EXPLAIN ANALYZE`,
                else run `EXPLAIN`. Consult the documentation for your dialect for an
                in-depth explanation of the two options.
            connection: optional
                A raw DBAPI connection object.
            format: defaults "json"
                If supported, the output format for the EXPLAIN result.
                Consult the documentation for your dialect to get a full list of options.
            **kwargs:
                Any keyword-arguments you'll pass on to the query.
        Returns:
            The raw results of the EXPLAIN query.
        """
        name = query if isinstance(query, str) else query.__name__
        queryfn: aiosql.types.QueryFn = getattr(self.queries, name)
        c: protos.ConnectionT
        async with self.connector.transaction(
            connection=connection, rollback=True
        ) as c:
            op = self.connector.get_explain_command(analyze, format)
            if op == self.connector.EXPLAIN_PREFIX:
                selector, sql = (
                    self.queries.driver_adapter.select_one,
                    f"{op}{queryfn.sql}",
                )
            else:
                selector, sql = (
                    self.queries.driver_adapter.select_value,
                    f"{op}{queryfn.sql}",
                )
            return await selector(
                c,
                query_name=name,
                sql=sql,
                parameters=kwargs or args,
            )

    @classmethod
    def get_kvs(cls, model: protos.ModelT) -> Mapping:
        """Get a mapping of key-value pairs for your model without excluded fields."""
        return {
            field: value
            for field, value in cls.protocol.iterate(model)
            if field not in cls.metadata.__exclude_fields__
        }

    @classmethod
    def _get_table_name(cls) -> str:
        """Get the name of the table for this query lib

        Overload this method to customize how your determine the table name of your
        Query library.

        Notes:
            This is run if Metadata.__tablename__ is not set by the user.
        """
        return inflection.underscore(cls.model.__name__)

    @classmethod
    def _get_query_library(cls):
        """Load the query library from disk into memory.

        Overload this method to customize how your query library is loaded.

        Notes:
            By default, this will join `Metadata.__querylib__` &
            `Metadata.__tablename__` as a path and attempt to load all sql files found.
        """
        path = cls.metadata.__querylib__ / cls.metadata.__tablename__
        lib = aiosql.from_path(path, cls.metadata.__driver__)
        return lib

    @classmethod
    def _bootstrap_user_queries(cls):
        """Bootstrap all raw Query functions and attach them to this service.

        Overload this method to customize how your queries are bootstrapped.
        """
        for name in cls.queries.available_queries:
            queryfn: QueryFn = getattr(cls.queries, name)
            bootstrapped = bootstrap(cls, queryfn)
            setattr(cls, name, bootstrapped)


def bootstrap(
    cls: Type[protos.ServiceProtocolT[_MT]], func: QueryFn
) -> protos.QueryMethodProtocol[_MT, protos.RawT]:
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
            func.__name__ in cls.metadata.__scalar_queries__ or support.isscalar(func)
        ),
    )
    bulk = cast(Literal[True, False], bool(not scalar and support.isbulk(func)))
    run_query: protos.QueryMethodProtocol[_MT, protos.RawT]
    if func.__name__.endswith("_cursor"):
        run_query = _bootstrap_cursor(func, scalar=scalar)  # type: ignore

    elif support.ispersist(func):
        run_query = _bootstrap_persist(func, scalar=scalar, bulk=bulk)  # type: ignore

    else:
        run_query = _bootstrap_default(func, scalar=scalar, bulk=bulk)  # type: ignore

    run_query.__name__ = func.__name__
    run_query.__doc__ = func.__doc__
    run_query.__qualname__ = f"{cls.__name__}.{func.__name__}"
    run_query.__module__ = cls.__module__
    return run_query


def _bootstrap_cursor(func: QueryFn, *, scalar: bool) -> protos.CursorMethodProtocolT:
    if scalar:

        @contextlib.asynccontextmanager
        async def run_scalar_query_cursor(
            self: protos.ServiceProtocolT,
            *args,
            connection: protos.ConnectionT = None,
            coerce: bool = False,
            **kwargs,
        ):
            async with self.connector.connection(c=connection) as c:
                async with func(c, *args, **kwargs) as cursor:
                    yield await cursor

        return cast(protos.CursorMethodProtocolT, run_scalar_query_cursor)

    @contextlib.asynccontextmanager
    async def run_query_cursor(
        self: protos.ServiceProtocolT,
        *args,
        connection: protos.ConnectionT = None,
        coerce: bool = True,
        **kwargs,
    ):
        async with self.connector.connection(c=connection) as c:
            async with func(c, *args, **kwargs) as factory:
                cursor = await factory
                yield CoercingCursor(self, cursor) if coerce else cursor

    return cast(protos.CursorMethodProtocolT, run_query_cursor)


@overload
def _bootstrap_persist(
    func: QueryFn, *, scalar: Literal[False], bulk: Literal[False]
) -> protos.ModelPersistProtocolT:
    ...


@overload
def _bootstrap_persist(
    func: QueryFn, *, scalar: Literal[True], bulk: Literal[False]
) -> protos.RawPersistProtocolT:
    ...


@overload
def _bootstrap_persist(
    func: QueryFn, *, scalar: Literal[True], bulk: Literal[True]
) -> protos.RawBulkPersistProtocolT:
    ...


@overload
def _bootstrap_persist(
    func: QueryFn, *, scalar: Literal[False], bulk: Literal[True]
) -> protos.BulkModelPersistProtocolT:
    ...


def _bootstrap_persist(
    func: QueryFn, *, scalar: Literal[True, False], bulk: Literal[True, False]
):
    if bulk is True:

        @support.retry
        async def run_persist_query(
            self: protos.ServiceProtocolT[protos.ModelT],
            *__,
            connection: protos.ConnectionT = None,
            models: Iterable[protos.ModelT] = (),
            data: Iterable[Mapping] = (),
            **___,
        ):
            if models:
                data = (self.get_kvs(m) for m in models)
            async with self.connector.transaction(connection=connection) as c:
                return await func(c, data)

    else:

        @support.retry
        async def run_persist_query(
            self: protos.ServiceProtocolT[protos.ModelT],
            *__,
            model: protos.ModelT = None,
            connection: protos.ConnectionT = None,
            **kwargs,
        ):
            data = kwargs
            if model:
                data.update(self.get_kvs(model))
            async with self.connector.transaction(connection=connection) as c:
                return await func(c, **data)

    if scalar is False:
        return support.coerceable(run_persist_query, bulk=bulk)

    return run_persist_query


@overload
def _bootstrap_default(
    func: QueryFn, *, scalar: Literal[False], bulk: Literal[False]
) -> protos.ModelProtocolT:
    ...


@overload
def _bootstrap_default(
    func: QueryFn, *, scalar: Literal[False], bulk: Literal[True]
) -> protos.BulkModelProtocolT:
    ...


@overload
def _bootstrap_default(
    func: QueryFn, *, scalar: Literal[True], bulk: Literal[False]
) -> protos.RawProtocolT:
    ...


@overload
def _bootstrap_default(
    func: QueryFn, *, scalar: Literal[True], bulk: Literal[True]
) -> protos.RawBulkProtocolT:
    ...


def _bootstrap_default(
    func: QueryFn, *, scalar: Literal[True, False], bulk: Literal[True, False]
):
    if support.ismutate(func):

        @support.retry
        async def run_default_query(
            self: protos.ServiceProtocolT[protos.ModelT],
            *args,
            connection: protos.ConnectionT = None,
            **kwargs,
        ):
            async with self.connector.transaction(connection=connection) as c:
                return await func(c, *args, **kwargs)

    else:

        @support.retry
        async def run_default_query(
            self: protos.ServiceProtocolT[protos.ModelT],
            *args,
            connection: protos.ConnectionT = None,
            **kwargs,
        ):
            async with self.connector.connection(c=connection) as c:
                return await func(c, *args, **kwargs)

    if scalar is False:
        return support.coerceable(run_default_query, bulk=bulk)

    return run_default_query


RawDefaultMethodT = Union[
    protos.RawProtocolT,
    protos.RawBulkProtocolT,
    protos.RawPersistProtocolT,
    protos.RawBulkPersistProtocolT,
]
