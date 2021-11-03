from __future__ import annotations

import importlib
import inspect
import functools
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
    Tuple,
)

import aiosql
import inflection
import typic
from aiosql.types import QueryFn
from . import drivers, support, types, bootstrap, inspection

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


class BaseQueryService(types.ServiceProtocolT[_MT]):
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

    __slots__ = ("connector", "_managed")

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
        self._managed = connector is None

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
        cls._bootstrap_middlewares()
        return super().__init_subclass__(**kwargs)

    def __class_getitem__(cls, item):
        cls.model = item
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
        driver = cls._DRIVER_TO_AIOSQL[cls.metadata.__driver__]
        if "." in driver:
            modname, clsname = driver.rsplit(".", maxsplit=1)
            mod = importlib.import_module(modname)
            driver = getattr(mod, clsname)
        lib = aiosql.from_path(path, driver)
        return lib

    _DRIVER_TO_AIOSQL = {
        "asyncpg": "asyncpg",
        "aiosqlite": "norma.core.drivers.aio.sqlite.AIOSQLiteReturningDriverAdaptor",
        "psycopg": "psycopg2",
        "sqlite": "norma.core.drivers.sio.sqlite.SQLite3ReturningDriverAdaptor",
    }

    @classmethod
    def _bootstrap_user_queries(cls):
        """Bootstrap all raw Query functions and attach them to this service.

        Overload this method to customize how your queries are bootstrapped.
        """
        cursor_proxy = support.get_cursor_proxy(cls.metadata.__driver__)
        for name in cls.queries.available_queries:
            # Allow users to override the default bootstrapping.
            if hasattr(cls, name):
                continue

            queryfn: QueryFn = getattr(cls.queries, name)
            bootstrapped = bootstrap.bootstrap(cls, queryfn, cursor_proxy=cursor_proxy)
            setattr(cls, name, bootstrapped)

    @classmethod
    def _bootstrap_middlewares(cls):
        for name, mware in cls._iter_middlewares():
            cls._bind_middleware(mware)

    @classmethod
    def _iter_middlewares(cls) -> Iterable[Tuple[str, types.MiddelwareMethodProtocolT]]:
        for name, call in inspect.getmembers(cls, inspection.ismiddleware):
            yield name, call

    @classmethod
    def _bind_middleware(cls, mware: types.MiddelwareMethodProtocolT):
        for qname in mware.__intercepts__:
            query: types.QueryMethodProtocolT = getattr(cls, qname)

            @functools.wraps(query)  # type: ignore
            def _wrap_query(
                self: types.ServiceProtocolT, *args, __mw=mware, __q=query, **kwargs
            ):
                return __mw(self, __q, *args, **kwargs)

            setattr(cls, qname, _wrap_query)

    @classmethod
    def _get_explain_selector(cls, op: str):
        return (
            cls.queries.driver_adapter.select_one
            if op == cls.connector.EXPLAIN_PREFIX
            else cls.queries.driver_adapter.select_value
        )

    def count(
        self,
        query: Union[str, Callable],
        *args,
        connection: types.ConnectionT = None,
        **kwargs,
    ):
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
        return self._do_count(connection, name, sql, kwargs or args)

    def explain(
        self,
        query: Union[str, Callable],
        *args,
        analyze: bool = True,
        connection: types.ConnectionT = None,
        format: Optional[ExplainFormatT] = "json",
        **kwargs,
    ):
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
        op = self.connector.get_explain_command(analyze, format)
        selector = self._get_explain_selector(op)
        sql = f"{op}{queryfn.sql}"
        return self._do_explain(connection, name, sql, selector, args or kwargs)

    def _do_count(
        self,
        connection: types.ConnectionT,
        name: str,
        sql: str,
        parameters: dict | tuple,
    ):
        ...

    def _do_explain(
        self,
        connection: types.ConnectionT,
        name: str,
        sql: str,
        selector: Callable,
        parameters: dict | tuple,
    ):
        ...


class AsyncQueryService(BaseQueryService[_MT]):
    """An event-loop compatible query service (async/await)."""

    connector: types.AsyncConnectorProtocolT

    async def __aenter__(self):
        await self.connector.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._managed:
            await self.connector.close()
        return

    @support.retry  # type: ignore
    async def _do_count(
        self,
        connection: types.ConnectionT,
        name: str,
        sql: str,
        parameters: dict | tuple,
    ) -> int:
        async with self.connector.connection(connection=connection) as c:
            return await self.queries.driver_adapter.select_value(
                c,
                query_name=name,
                sql=sql,
                parameters=parameters,
            )

    @support.retry  # type: ignore
    async def _do_explain(
        self,
        connection: types.ConnectionT,
        name: str,
        sql: str,
        selector: Callable,
        parameters: dict | tuple,
    ) -> Union[types.ScalarT, str]:
        c: types.ConnectionT
        async with self.connector.transaction(
            connection=connection, rollback=True
        ) as c:
            return await selector(c, query_name=name, sql=sql, parameters=parameters)


class SyncQueryService(BaseQueryService[_MT]):
    """A blocking-IO query service."""

    connector: types.SyncConnectorProtocolT

    __abstract__ = True

    class metadata(QueryMetadata):
        __driver__: ClassVar[Literal["psycopg", "sqlite"]] = "psycopg"

    def __enter__(self):
        self.connector.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._managed:
            self.connector.close()
        return

    @support.retry  # type: ignore
    def _do_count(
        self,
        connection: Optional[types.ConnectionT],
        name: str,
        sql: str,
        parameters: dict | tuple,
    ) -> int:
        with self.connector.connection(connection=connection) as c:
            return self.queries.driver_adapter.select_value(
                c,
                query_name=name,
                sql=sql,
                parameters=parameters,
            )

    @support.retry  # type: ignore
    def _do_explain(
        self,
        connection: Optional[types.ConnectionT],
        name: str,
        sql: str,
        selector: Callable,
        parameters: dict | tuple,
    ) -> Union[types.ScalarT, str]:
        with self.connector.transaction(connection=connection, rollback=True) as c:
            return selector(c, query_name=name, sql=sql, parameters=parameters)
