from __future__ import annotations

import inspect
import logging
import pathlib
from typing import (
    Iterable,
    Union,
    Any,
    Callable,
    Literal,
    ClassVar,
    FrozenSet,
    Type,
    Optional,
    TypeVar,
    Dict,
)

import aiosql
import inflection
import typic
from aiosql.types import QueryFn
from . import drivers, support, types, bootstrap

__all__ = (
    "AsyncQueryService",
    "BaseQueryService",
    "ExplainFormatT",
    "QueryMetadata",
    "SyncQueryService",
)

logger = logging.getLogger(__name__)


ExplainFormatT = Literal["json", "yaml", "xml"]

_MT = TypeVar("_MT")
_CT = TypeVar("_CT", bound=types.ConnectorProtocol)
_ConnT = TypeVar("_ConnT")
_RT = TypeVar("_RT")


class QueryMetadata(types.MetadataT):
    __slots__ = ()
    __driver__: ClassVar[drivers.SupportedDriversT] = "asyncpg"
    __exclude_fields__: ClassVar[FrozenSet[str]] = frozenset(
        ("id", "created_at", "updated_at")
    )
    __scalar_queries__: ClassVar[FrozenSet[str]] = frozenset(())
    __querylib__: ClassVar[Union[str, pathlib.Path]]


class BaseQueryService(
    types.ServiceProtocolT[_MT, types.AnyConnectorProtocolT[types.ConnectionT]]
):
    """The base class for a 'service'.

    A 'service' is responsible for querying a specific table.
    It also is semi-aware of in-memory dataclass representing the table data.

    By default, a service will coerce the query result to the bound model.
    """

    # User-defined class attributes
    model: ClassVar[Type[_MT]] = Any  # type: ignore
    metadata: ClassVar[Type[types.MetadataT]] = QueryMetadata
    # Generated attributes
    protocol: ClassVar[typic.SerdeProtocol[_MT]]
    bulk_protocol: ClassVar[typic.SerdeProtocol[Iterable[_MT]]]
    queries: ClassVar[aiosql.aiosql.Queries]

    __slots__ = ("connector",)

    __getattr__: Callable[..., types.QueryMethodProtocolT[_MT]]

    def __init__(
        self,
        *,
        connector: types.AnyConnectorProtocolT[types.ConnectionT] = None,
        **connect_kwargs,
    ):
        self.connector = connector or support.get_connector_protocol(
            self.metadata.__driver__, **connect_kwargs
        )

    def __init_subclass__(cls, **kwargs):
        if cls.__name__ in {"AsyncQueryService", "SyncQueryService"}:
            return super().__init_subclass__(**kwargs)

        for pcls in inspect.getmro(cls)[1:]:  # the first entry is always `cls`
            if not issubclass(pcls, BaseQueryService):
                continue
            cls.metadata.__exclude_fields__ |= pcls.metadata.__exclude_fields__
            cls.metadata.__scalar_queries__ |= pcls.metadata.__scalar_queries__

        if not hasattr(cls.metadata, "__tablename__"):
            cls.metadata.__tablename__ = cls._get_table_name()

        cls.queries = cls._get_query_library()
        cls.protocol = typic.protocol(cls.model, is_optional=True)
        cls.bulk_protocol = typic.protocol(Iterable[cls.model])
        cls._bootstrap_user_queries()
        return super().__init_subclass__(**kwargs)

    def __class_getitem__(cls, item):
        modelt = item
        if isinstance(item, tuple):
            modelt, *_ = item

        cls.model = modelt
        return super().__class_getitem__(item)

    @classmethod
    def get_kvs(cls, model: types.ModelT) -> Dict[str, Any]:
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
        return inflection.underscore(typic.get_name(cls.model))

    @classmethod
    def _get_query_library(cls):
        """Load the query library from disk into memory.

        Overload this method to customize how your query library is loaded.

        Notes:
            By default, this will join `Metadata.__querylib__` &
            `Metadata.__tablename__` as a path and attempt to load all sql files found.
        """
        if not hasattr(cls.metadata, "__querylib__"):
            cls.metadata.__querylib__ = pathlib.Path.cwd().resolve()
        path = cls.metadata.__querylib__ / cls.metadata.__tablename__
        driver = cls.metadata.__driver__
        if driver == "psycopg":
            driver = "psycopg2"
        lib = aiosql.from_path(path, driver)
        return lib

    @classmethod
    def _bootstrap_user_queries(cls):
        """Bootstrap all raw Query functions and attach them to this service.

        Overload this method to customize how your queries are bootstrapped.
        """
        for name in cls.queries.available_queries:
            queryfn: QueryFn = getattr(cls.queries, name)
            bootstrapped = bootstrap.bootstrap(cls, queryfn)
            setattr(cls, name, bootstrapped)


class AsyncQueryService(BaseQueryService[_MT, types.ConnectionT]):
    """An event-loop compatible query service (async/await)."""

    connector: types.AsyncConnectorProtocolT[types.ConnectionT]

    async def __aenter__(self):
        await self.connector.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return

    @support.retry  # type: ignore
    async def count(
        self,
        query: Union[str, Callable],
        *args,
        connection: types.ConnectionT = None,
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

    @support.retry  # type: ignore
    async def explain(
        self,
        query: Union[str, Callable],
        *args,
        analyze: bool = True,
        connection: types.ConnectionT = None,
        format: Optional[ExplainFormatT] = "json",
        **kwargs,
    ) -> Union[types.ScalarT, str]:
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
        c: types.ConnectionT
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


class SyncQueryService(BaseQueryService[_MT, types.ConnectionT]):
    """A blocking-IO query service."""

    connector: types.SyncConnectorProtocolT[types.ConnectionT]

    __abstract__ = True

    class metadata(QueryMetadata):
        __driver__: ClassVar[Literal["psycopg", "sqlite"]] = "psycopg"

    def __enter__(self):
        self.connector.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    @support.retry
    def count(
        self,
        query: Union[str, Callable],
        *args,
        connection: types.ConnectionT = None,
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
        with self.connector.connection(c=connection) as c:
            return self.queries.driver_adapter.select_value(
                c,
                query_name=name,
                sql=sql,
                parameters=kwargs or args,
            )

    @support.retry
    def explain(
        self,
        query: Union[str, Callable],
        *args,
        analyze: bool = True,
        connection: types.ConnectionT = None,
        format: Optional[ExplainFormatT] = "json",
        **kwargs,
    ) -> Union[types.ScalarT, str]:
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
        c: types.ConnectionT
        with self.connector.transaction(connection=connection, rollback=True) as c:
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
            return selector(
                c,
                query_name=name,
                sql=sql,
                parameters=kwargs or args,
            )
