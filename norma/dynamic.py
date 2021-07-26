import contextlib
from typing import (
    Union,
    AsyncContextManager,
    Any,
    Awaitable,
    TypeVar,
    Mapping,
    AsyncIterator,
    Iterable,
)

import pypika

from norma import support, protos, client, drivers

_MT = TypeVar("_MT")
_RT = TypeVar("_RT")


class DynamicQueryLib:
    __slots__ = ("service", "table", "builder")

    def __init__(self, service: protos.ServiceProtocolT, *, schema: str = None):
        self.service = service
        self.table = self._get_table(service.metadata.__tablename__, schema=schema)
        self.builder = self._get_query_builder(
            self.table, driver=service.metadata.__driver__
        )

    @support.coerceable(bulk=True)
    @support.retry
    async def execute(
        self,
        query: Union[str, pypika.queries.QueryBuilder],
        *args,
        connection: protos.ConnectionT = None,
        coerce: bool = True,
        **kwargs,
    ) -> Iterable:
        if isinstance(query, pypika.queries.QueryBuilder):
            query = query.get_sql()

        async with self.service.connector.connection(c=connection) as c:
            return await self.service.queries.driver_adapter.select(
                conn=c, query_name="all", sql=query, parameters=args or kwargs
            )

    @contextlib.asynccontextmanager
    async def execute_cursor(
        self,
        query: Union[str, pypika.queries.QueryBuilder],
        *args,
        connection: protos.ConnectionT = None,
        coerce: bool = True,
        **kwargs,
    ) -> AsyncIterator[protos.CursorProtocolT]:
        if isinstance(query, pypika.queries.QueryBuilder):
            query = query.get_sql()

        async with self.service.connector.connection(c=connection) as c:
            async with self.service.queries.driver_adapter.select_cursor(
                conn=c, query_name="all", sql=query, parameters=args or kwargs
            ) as factory:
                cursor = await factory
                yield client.CoercingCursor(self.service, cursor) if coerce else cursor

    def select(
        self,
        *fields,
        connection: protos.ConnectionT = None,
        coerce: bool = True,
        **where: Any,
    ) -> Awaitable[Iterable]:
        query = self.build_select(*fields, **where)
        return self.execute(query, connection=connection, coerce=coerce)

    def select_cursor(
        self,
        *fields,
        connection: protos.ConnectionT = None,
        coerce: bool = True,
        **where: Any,
    ) -> AsyncContextManager[protos.CursorProtocolT]:
        query = self.build_select(*fields, **where)
        return self.execute_cursor(query, connection=connection, coerce=coerce)

    def build_select(self, *fields: str, **where: Any) -> pypika.queries.QueryBuilder:
        fs: Iterable[str] = fields or [self.builder.star]
        query: pypika.queries.QueryBuilder = self.builder.select(*fs).where(
            pypika.Criterion.all(
                [getattr(self.table, c) == v for c, v in where.items()]
            )
        )
        return query

    @staticmethod
    def _get_table(name: str, *, schema: str = None) -> pypika.Table:
        return pypika.Table(name, schema=schema)

    @classmethod
    def _get_query_builder(
        cls, table: pypika.Table, *, driver: drivers.SupportedDriversT = "asyncpg"
    ) -> pypika.queries.QueryBuilder:
        return pypika.Query.from_(table, dialect=cls._DRIVERS_TO_DIALECT[driver])

    _DRIVERS_TO_DIALECT: Mapping[drivers.SupportedDriversT, pypika.Dialects] = {
        "asyncpg": pypika.Dialects.POSTGRESQL,
        "aiosqlite": pypika.Dialects.SQLLITE,
    }
