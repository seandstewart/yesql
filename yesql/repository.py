from __future__ import annotations

import collections
import dataclasses
import inspect
import logging
import pathlib
from types import SimpleNamespace
from typing import (
    Any,
    ClassVar,
    Deque,
    Dict,
    FrozenSet,
    Iterable,
    Literal,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import inflection
import typic

from yesql import statement
from yesql.core import drivers, middleware, parse, types

__all__ = (
    "AsyncQueryRepository",
    "BaseQueryRepository",
    "ExplainFormatT",
    "QueryMetadata",
    "SyncQueryRepository",
)

logger = logging.getLogger(__name__)


ExplainFormatT = Literal["json", "yaml", "xml"]

_MT = TypeVar("_MT")
_ConnT = TypeVar("_ConnT")
_RT = TypeVar("_RT")


class QueryMetadata(types.MetadataT):
    """Default metadata for the query repository.

    Query Metadata provides simple configuration for binding an execution Statement,
    a parsed QueryDatum, and a repository's data model.
    """

    __slots__ = ()
    __dialect__: ClassVar[drivers.SupportedDialectsT] = "postgresql"
    __exclude_fields__: ClassVar[FrozenSet[str]] = frozenset(
        ("id", "created_at", "updated_at")
    )
    __querylib__: ClassVar[Union[str, pathlib.Path]]


class BaseQueryRepository(types.RepositoryProtocolT[_MT]):
    """The base class for a 'repository'.

    A 'repository' is responsible for querying a specific table.
    It also is semi-aware of in-memory dataclass representing the table data.

    By default, a repository will coerce the query result to the bound model.
    """

    # User-defined class attributes
    model: ClassVar[Type[_MT]] = Any  # type: ignore
    metadata: ClassVar[Type[types.MetadataT]] = QueryMetadata
    # Generated attributes
    queries: ClassVar[parse.QueryPackage]
    driver: ClassVar[drivers.Driver]
    # Generated or Initialized Attributes
    executor: drivers.BaseQueryExecutor
    serdes: statement.SerDes[_MT]
    # Private attributes.
    _protocol: ClassVar[typic.SerdeProtocol[_MT | None]]
    _bulk_protocol: ClassVar[typic.SerdeProtocol[Iterable[_MT]]]

    __slots__ = ()

    __statements__: dict[str, statement.Statement[_MT]]

    def __init__(
        self,
        *,
        executor: drivers.BaseQueryExecutor = None,
        serdes: statement.SerDes[_MT] = None,
        **connect_kwargs,
    ):
        # If we're overriding the default connector, then propagate it.
        if executor:
            self.executor = executor
            for stat in self.__statements__.values():
                stat.executor = self.executor
        # If we're overriding the default serdes, then propagate it.
        if serdes:
            self.serdes = serdes
            for stat in self.__statements__.values():
                stat.serdes = self.serdes
        # Ingest custom connection args.
        # This will only matter if we haven't already connected to the database.
        self.executor.pool_kwargs.update(connect_kwargs)

    def initialize(self):
        """Initialize the query executor's connection to the underlying database."""
        return self.executor.initialize()

    def teardown(self, *, timeout: int = 10):
        """Tear down the query executor's connection to the underlying database."""
        return self.executor.teardown(timeout=timeout)

    def __init_subclass__(cls, **kwargs):
        if cls.__name__ in {"AsyncQueryRepository", "SyncQueryRepository"}:
            return super().__init_subclass__(**kwargs)

        for pcls in inspect.getmro(cls)[1:]:  # the first entry is always `cls`
            if not issubclass(pcls, BaseQueryRepository):
                continue
            cls.metadata.__exclude_fields__ |= pcls.metadata.__exclude_fields__

        if not hasattr(cls.metadata, "__tablename__"):
            cls.metadata.__tablename__ = cls._get_table_name()

        cls._protocol = typic.protocol(cls.model, is_optional=True)
        cls._bulk_protocol = typic.protocol(Iterable[cls.model])
        cls.serdes = statement.SerDes(
            serializer=cls.get_kvs,
            deserializer=cls._protocol.transmute,
            bulk_deserializer=cls._bulk_protocol.transmute,
        )
        cls.driver = drivers.get_driver(dialect=cls.metadata.__dialect__, aio=cls.isaio)
        cls.executor = cls.driver.executor()
        cls.queries = cls._get_query_library()
        cls.__statements__ = cls._resolve_statements()
        return super().__init_subclass__(**kwargs)

    @classmethod
    def get_kvs(cls, model: types.ModelT) -> Dict[str, Any]:
        """Get a mapping of key-value pairs for your model without excluded fields."""
        return {
            field: value
            for field, value in cls._protocol.iterate(model)
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
    def _get_query_library(cls) -> parse.QueryPackage:
        """Load the query library from disk into memory.

        Overload this method to customize how your query library is loaded.

        Notes:
            By default, this will join `Metadata.__querylib__` &
            `Metadata.__tablename__` as a path and attempt to load all sql files found.
        """
        if not hasattr(cls.metadata, "__querylib__"):
            cls.metadata.__querylib__ = pathlib.Path.cwd().resolve()
        if isinstance(cls.metadata.__querylib__, str):
            cls.metadata.__querylib__ = pathlib.Path(
                cls.metadata.__querylib__
            ).resolve()
        path = cls.metadata.__querylib__ / cls.metadata.__tablename__
        lib = parse.parse(path, driver=cls.driver.executor.__driver__)
        return lib

    @classmethod
    def _resolve_statements(cls) -> dict[str, statement.Statement]:
        """Bootstrap all raw Query functions and attach them to this service.

        Overload this method to customize how your queries are bootstrapped.
        """
        mwares = {
            qname: mware
            for _, mware in cls._iter_middlewares()
            for qname in mware.__intercepts__
        }
        available = cls._bootstrap_package(cls.queries, mwares)
        for name, stmt in available.items():
            # Don't override a custom impl, but let them use it if they want.
            if hasattr(cls, name):
                name = name + "_default"
            setattr(cls, name, stmt)
        stack: _QueryPackageStack = collections.deque(
            (cls, pkg) for pkg in cls.queries.packages.values()
        )
        # Build the tree of queries associated to this repository.
        while stack:
            parent, package = stack.popleft()
            queries = cls._bootstrap_package(package, mwares)
            ns = SimpleNamespace(**queries)
            setattr(parent, package.name, ns)
            stack.extend((ns, pkg) for pkg in package.packages.values())
            available.update(queries)

        return available

    @classmethod
    def _bootstrap_package(
        cls,
        package: parse.QueryPackage,
        middlewares: dict[str, types.MiddlewareMethodProtocolT],
    ) -> dict[str, statement.Statement]:
        available = {}
        # For every module in the package...
        for mname, module in package.modules.items():
            # For every query in the module...
            for name, datum in module.queries.items():
                # Get the unit-of-work statements for this query
                statements = statement.statements(
                    datum,
                    executor=cls.executor,
                    serdes=cls.serdes,
                )
                # For every statement...
                for stat in statements:
                    # Check if there is an associated middleware
                    mware = middlewares.get(stat.query.name)
                    if mware:
                        stat.middleware = mware
                    # Add the statement to the mapping of available units of work
                    #   for this package.
                    available[stat.query.name] = stat
        return available

    @classmethod
    def _iter_middlewares(cls) -> Iterable[tuple[str, types.MiddlewareMethodProtocolT]]:
        for name, call in inspect.getmembers(cls, middleware.ismiddleware):
            yield name, call

    def count(
        self,
        query: Union[str, statement.Statement],
        *args,
        estimate_ok: bool = True,
        **kwargs,
    ):
        """Get the number of rows returned by this query.

        Args:
            query:
                Either the query function, or the name of the query in your library
                which you wish to get a count from.
            *args:
                Any positional arguments which the query requires.
            **kwargs:
                Any keyword-arguments you'll pass on to the query.

        Returns:
            The number of rows.
        """
        if isinstance(query, str):
            query = cast(statement.Statement, getattr(self, query))
        datum = query.query
        sql = f"SELECT count(*) FROM ({query.query.sql.rstrip(';')}) AS q;"
        stat = dataclasses.replace(datum, sql=sql)
        return self.executor.scalar(stat, *args, **kwargs)

    def explain(
        self,
        query: Union[str, statement.Statement],
        *args,
        analyze: bool = True,
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

            The exact command run is determined by the Executor's
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
            format: defaults "json"
                If supported, the output format for the EXPLAIN result.
                Consult the documentation for your dialect to get a full list of options.
            **kwargs:
                Any keyword-arguments you'll pass on to the query.
        Returns:
            The raw results of the EXPLAIN query.
        """
        if isinstance(query, str):
            query = cast(statement.Statement, getattr(self, query))
        datum = query.query
        op = self.executor.get_explain_command(analyze, format)
        sql = f"{op}{datum.sql}"
        stat = dataclasses.replace(datum, sql=sql)
        kwargs.update(transaction=True, rollback=True)
        if op == self.executor.EXPLAIN_PREFIX:
            kwargs["coerce"] = False
            return self.executor.one(
                stat,
                *args,
                **kwargs,
            )

        return self.executor.scalar(
            stat,
            *args,
            **kwargs,
        )


class AsyncQueryRepository(BaseQueryRepository[_MT]):
    """An event-loop compatible query repository (async/await)."""

    isaio = True

    async def __aenter__(self):
        await self.executor.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.executor.teardown()
        return


class SyncQueryRepository(BaseQueryRepository[_MT]):
    """A blocking-IO query repository."""

    isaio = False

    def __enter__(self):
        self.executor.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.executor.teardown()
        return


_QueryNamespaceT = Union[Type[types.RepositoryProtocolT], SimpleNamespace]
_QueryPackageStack = Deque[Tuple[_QueryNamespaceT, parse.QueryPackage]]
