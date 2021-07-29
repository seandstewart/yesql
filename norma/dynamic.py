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
    """Query Library for building ad-hoc queries in-memory.

    This service acts as glue between the `pypika` query-builder library and Norma's
    `QueryService`.

    It is intended as an escape-hatch for the small subset of situations where
    dynamically-built queries are  actually needed (think an admin tool, like
    Flask-Admin, for instance).
    """

    __slots__ = (
        "service",
        "protocol",
        "bulk_protocol",
        "connector",
        "table",
        "builder",
    )

    def __init__(self, service: protos.ServiceProtocolT, *, schema: str = None):
        self.service = service
        self.protocol = self.service.protocol
        self.bulk_protocol = self.service.bulk_protocol
        self.connector = self.service.connector
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
        **kwargs,
    ) -> Iterable:
        """Execute any arbitrary query and return the result.

        Notes:
            This method assumes that the result is coerceable into the model bound to
            the QueryService. If that is not the case, then make sure to pass in
            `coerce=False` to the call.

        Args:
            query:
                Either a SQL string or a pypika Query.
            *args:
                Any args which should be passed on to the query.
            connection: optional
                A DBAPI connectable to use during executions.
                Whether to coerce the query result into the model bound to the service.
            **kwargs:
                Any keyword args to pass on to the query.
        Keyword Args:
            coerce: bool, defaults True
        Returns:

        """
        if isinstance(query, pypika.queries.QueryBuilder):
            query = query.get_sql()

        async with self.service.connector.transaction(c=connection) as c:
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
        """Execute any arbitrary query and enter a cursor context.

        Args:
            query:
                Either a SQL string or a pypika Query.
            *args:
                Any args which should be passed on to the query.
            connection: optional
                A DBAPI connectable to use during executions.
                Whether to coerce the query result into the model bound to the service.
            coerce: defaults True
            **kwargs:
                Any keyword args to pass on to the query.
        """
        if isinstance(query, pypika.queries.QueryBuilder):
            query = query.get_sql()

        async with self.service.connector.transaction(c=connection) as c:
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
        """A convenience method for executing an arbitrary SELECT query.

        Notes:
            This method assumes that the result is coerceable into the model bound to
            the QueryService. If that is not the case, then make sure to pass in
            `coerce=False` to the call.

        Args:
            *fields:
                Optionally specify specific fields to return.
            connection: optional
                A DBAPI connectable to use during executions.
                Whether to coerce the query result into the model bound to the service.
            coerce: defaults True
            **where:
                Optinally specify direct equality comparisions for the WHERE clause.
        """
        query = self.build_select(*fields, **where)
        return self.execute(query, connection=connection, coerce=coerce)

    def select_cursor(
        self,
        *fields,
        connection: protos.ConnectionT = None,
        coerce: bool = True,
        **where: Any,
    ) -> AsyncContextManager[protos.CursorProtocolT]:
        """A convenience method for executing an arbitrary SELECT query and entering a cursor context.

        Notes:
            This method assumes that the result is coerceable into the model bound to
            the QueryService. If that is not the case, then make sure to pass in
            `coerce=False` to the call.

        Args:
            *fields:
                Optionally specify specific fields to return.
            connection: optional
                A DBAPI connectable to use during executions.
                Whether to coerce the query result into the model bound to the service.
            coerce: defaults True
            **where:
                Optionally specify direct equality comparisions for the WHERE clause.
        """
        query = self.build_select(*fields, **where)
        return self.execute_cursor(query, connection=connection, coerce=coerce)

    def build_select(self, *fields: str, **where: Any) -> pypika.queries.QueryBuilder:
        """A convenience method for building a simple SELECT statement.

        Args:
            *fields:
                Optionally specify specific fields to return.
            **where:
                Optionally specify direct equality comparisions for the WHERE clause.
        """
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
