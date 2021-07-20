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
    Sequence,
)

import aiosql
import inflection
import typic
from aiosql.types import QueryFn

from norma import protos, support

logger = logging.getLogger(__name__)


ExplainFormatT = Literal["json", "yaml", "xml"]

_MT = TypeVar("_MT")
_RT = TypeVar("_RT")


class CoercingCursor(protos.CursorProtocolT[_MT]):
    """A shim around a cursor which will automatically convert data to the model."""

    __slots__ = ("service", "cursor")

    def __init__(
        self,
        service: protos.ServiceProtocolT[_MT, _RT],
        cursor: protos.CursorProtocolT[_RT],
    ):
        self.service = service
        self.cursor = cursor

    def __getattr__(self, item: str):
        return self.cursor.__getattribute__(item)

    async def forward(self, n: int, *args, timeout: float = None, **kwargs):
        return await self.cursor.forward(n, *args, timeout=timeout, **kwargs)

    async def fetch(
        self, n: int, *args, timeout: float = None, **kwargs
    ) -> Sequence[_MT]:
        page = await self.cursor.fetch(n, *args, timeout=timeout, **kwargs)
        return page and self.service.bulk_protocol(({**r} for r in page))

    async def fetchrow(self, *args, timeout: float = None, **kwargs) -> _MT:
        row = await self.cursor.fetchrow(*args, timeout=timeout, **kwargs)
        return row and self.service.protocol({**row})


class Metadata(protos.MetadataT):
    __slots__ = ()
    __driver__ = "asyncpg"
    __primary_key__ = "id"
    __exclude_fields__ = frozenset(("id", "created_at", "updated_at"))
    __scalar_queries__: ClassVar[FrozenSet[str]] = frozenset(())
    __querylib__: ClassVar[Union[str, pathlib.Path]]


class QueryService(Generic[_MT, _RT]):
    """The base class for a 'service'.

    A 'service' is responsible for querying a specific table.
    It also is semi-aware of in-memory dataclass representing the table data.

    By default, a service will coerce the query result to the correct model.
    """

    # User-defined class attributes
    model: ClassVar[Type[_MT]] = Any
    metadata: ClassVar[Type[Metadata]] = Metadata
    # Generated attributes
    protocol: ClassVar[typic.SerdeProtocol[_MT]]
    bulk_protocol: ClassVar[typic.SerdeProtocol[Iterable[_MT]]]
    queries: ClassVar[aiosql.aiosql.Queries]

    __slots__ = ("connector",)

    def __init__(
        self,
        *,
        connector: protos.ConnectorProtocol[_RT] = None,
        **connect_kwargs,
    ):
        self.connector: protos.ConnectorProtocol[
            _RT
        ] = connector or support.get_connector_protocol(
            self.metadata.__driver__, **connect_kwargs
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

    @property
    def pk(self) -> str:
        return self.metadata.__primary_key__

    @support.retry
    async def count(
        self,
        query: Union[str, Callable],
        *args,
        connection: protos.ConnectionT = None,
        **kwargs,
    ) -> int:
        name = query if isinstance(query, str) else query.__name__
        query = getattr(self.queries, name)
        sql = f"SELECT count(*) FROM ({query.sql.rstrip(';')}) AS q;"
        async with self.connector.connection(c=connection) as c:
            return await self.queries.driver_adapter.select_value(
                c,
                query_name=query.__name__,
                sql=sql,
                parameters=kwargs or args,
            )

    @support.retry
    async def explain(
        self,
        query: Union[str, Callable],
        *args,
        connection: protos.ConnectionT = None,
        format: Optional[ExplainFormatT] = "json",
        **kwargs,
    ) -> Union[protos.RawT, str]:
        name = query if isinstance(query, str) else query.__name__
        query = getattr(self.queries, name)
        c: protos.ConnectionT
        async with self.connector.connection(c=connection) as c:
            transaction = c.transaction()
            await transaction.start()
            try:
                selector, sql = (
                    self.queries.driver_adapter.select_one,
                    f"EXPLAIN ANALYZE {query.sql}",
                )
                if format:
                    selector, sql = (
                        self.queries.driver_adapter.select_value,
                        f"EXPLAIN (FORMAT {format}) {query.sql}",
                    )
                return await selector(
                    c,
                    query_name=query.__name__,
                    sql=sql,
                    parameters=kwargs or args,
                )
            finally:
                await transaction.rollback()

    @classmethod
    def get_kvs(cls, model: protos.ModelT) -> Mapping:
        return {
            field: value
            for field, value in cls.protocol.iterate(model)
            if field not in cls.metadata.__exclude_fields__
        }

    @classmethod
    def _get_table_name(cls) -> str:
        return inflection.underscore(cls.model.__name__)

    @classmethod
    def _get_query_library(cls):
        lib = aiosql.from_path(cls.metadata.__querylib__, cls.metadata.__tablename__)
        return lib

    @classmethod
    def _bootstrap_user_queries(cls):
        for name in cls.queries.available_queries:
            queryfn: QueryFn = getattr(cls.queries, name)
            bootstrapped = bootstrap(cls, queryfn)
            setattr(cls, name, bootstrapped)


def bootstrap(cls: Type[protos.ServiceProtocolT[_MT, _RT]], func: QueryFn):
    scalar = func.__name__ in cls.metadata.__scalar_queries__ or support.isscalar(func)
    bulk = not scalar and support.isbulk(func)
    if func.__name__.endswith("_cursor"):
        run_query = _bootstrap_cursor(func, scalar=scalar)

    elif support.ispersist(func):
        run_query = _bootstrap_persist(func, scalar=scalar, bulk=bulk)

    else:
        run_query = _bootstrap_default(func, scalar=scalar, bulk=bulk)

    run_query.__name__ = func.__name__
    run_query.__doc__ = func.__doc__
    run_query.__qualname__ = f"{cls.__name__}.{func.__name__}"
    run_query.__module__ = cls.__module__
    return run_query


def _bootstrap_cursor(func: QueryFn, *, scalar: bool):
    if scalar:

        @support.retry
        @contextlib.asynccontextmanager
        async def run_query(
            self: protos.ServiceProtocolT,
            *args,
            connection: protos.ConnectionT = None,
            **kwargs,
        ):
            async with self.connector.connection(c=connection) as c:
                async with func(c, *args, **kwargs) as cursor:
                    yield await cursor

        return run_query

    @support.retry
    @contextlib.asynccontextmanager
    async def run_query(
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


def _bootstrap_persist(func: QueryFn, *, scalar: bool, bulk: bool):
    @support.retry
    async def run_query(
        self: protos.ServiceProtocolT,
        *,
        model: protos.ModelT = None,
        connection: protos.ConnectionT = None,
        coerce: bool = True,
        **data,
    ):
        if model:
            data = self.get_kvs(model)
        async with self.connector.connection(c=connection) as c:
            return await func(c, *data)

    if not scalar:
        run_query = support.coerceable(run_query, bulk=bulk)

    return run_query


def _bootstrap_default(func: QueryFn, *, scalar: bool, bulk: bool):
    @support.retry
    async def run_query(
        self: protos.ServiceProtocolT,
        *args,
        connection: protos.ConnectionT = None,
        coerce: bool = True,
        **kwargs,
    ):
        async with self.connector.connection(c=connection) as c:
            return await func(c, *args, **kwargs)

    if not scalar:
        run_query = support.coerceable(run_query, bulk=bulk)

    return run_query
