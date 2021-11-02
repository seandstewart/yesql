import contextlib
import warnings
from typing import (
    Union,
    AsyncContextManager,
    Any,
    Awaitable,
    TypeVar,
    Mapping,
    AsyncIterator,
    Iterable,
    Generic,
    Iterator,
    ContextManager,
    Tuple,
    Literal,
    Callable,
)

import pypika

from norma.core import support, types, drivers, bootstrap, service

__all__ = ("AsyncDynamicQueryLib", "SyncDynamicQueryLib")

_MT = TypeVar("_MT")
_RT = TypeVar("_RT")
_ConnT = TypeVar("_ConnT")
_CtxT = TypeVar("_CtxT")
_ReturnT = Union[Iterable[_MT], Iterable[types.ScalarT]]


class BaseDynamicQueryLib(Generic[_MT]):
    """Query Library for building ad-hoc queries in-memory.

    This service acts as glue between the `pypika` query-builder library and Norma's
    `QueryService`.

    It is intended as an escape-hatch for the small subset of situations where
    dynamically-built queries are actually needed (think an admin tool, like
    Flask-Admin, for instance).
    """

    _QNAME = "execute"

    __slots__ = (
        "service",
        "protocol",
        "bulk_protocol",
        "connector",
        "table",
        "builder",
    )

    def __init__(
        self,
        service: types.ServiceProtocolT[_MT],
        *,
        schema: str = None,
    ):
        self.service = service
        self.protocol = self.service.protocol
        self.bulk_protocol = self.service.bulk_protocol
        self.connector = self.service.connector
        self.table = self._get_table(service.metadata.__tablename__, schema=schema)
        self.builder = self._get_query_builder(
            self.table, driver=service.metadata.__driver__
        )

    def execute(
        self,
        query: Union[str, pypika.queries.QueryBuilder],
        *args,
        connection: _ConnT = None,
        coerce: bool = True,
        rtype: Literal["all", "one", "val"] = "all",
        **kwargs,
    ) -> Union[_ReturnT, Awaitable[_ReturnT]]:
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
            coerce: defaults True
                Whether to coerce the query result into the model bound to the service.
            rtype: One of "all", "one", "val"; defaults "all"
                Fetch all rows, one row, or the first value in the first row.
                If "val", `coerce` will always evaluate to False.
           **kwargs:
               Any keyword args to pass on to the query.
        Keyword Args:
           coerce: bool, defaults True
        Returns:
            The query result.
        """
        query, params = self._resolve_query(query, args, kwargs)
        operation = self.service.queries.driver_adapter.select
        if rtype == "one":
            operation = self.service.queries.driver_adapter.select_one
        elif rtype == "val":
            operation = self.service.queries.driver_adapter.select_value
            coerce = False
        return self._do_execute(
            query, params, operation, connection=connection, coerce=coerce
        )

    def execute_cursor(
        self,
        query: Union[str, pypika.queries.QueryBuilder],
        *args,
        connection: _ConnT = None,
        coerce: bool = True,
        **kwargs,
    ) -> Union[
        ContextManager[types.SyncCursorProtocolT[_MT]],
        AsyncContextManager[types.AsyncCursorProtocolT[_MT]],
    ]:
        """Execute any arbitrary query and enter a cursor context.

        Args:
            query:
                Either a SQL string or a pypika Query.
            *args:
                Any args which should be passed on to the query.
            connection: optional
                A DBAPI connectable to use during executions.
            coerce: defaults True
                Whether to coerce the query result into the model bound to the service.
            **kwargs:
                Any keyword args to pass on to the query.
        """
        query, params = self._resolve_query(query, args, kwargs)
        return self._do_execute_cursor(
            query, params, connection=connection, coerce=coerce
        )

    def select(
        self,
        *fields,
        connection: _ConnT = None,
        coerce: bool = True,
        rtype: Literal["all", "one", "val"] = "all",
        **where: Any,
    ) -> Union[_ReturnT, Awaitable[_ReturnT]]:
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
            coerce: defaults True
                Whether to coerce the query result into the model bound to the service.
            rtype: One of "all", "one", "val"; defaults "all"
                Fetch all rows, one row, or the first value in the first row.
                If "val", `coerce` will always evaluate to False.
            **where:
                Optinally specify direct equality comparisions for the WHERE clause.
        """
        query = self.build_select(*fields, **where)
        return self.execute(query, connection=connection, coerce=coerce, rtype=rtype)

    def select_cursor(
        self,
        *fields,
        connection: _ConnT = None,
        coerce: bool = True,
        **where: Any,
    ):
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
            coerce: defaults True
                Whether to coerce the query result into the model bound to the service.
            **where:
                Optionally specify direct equality comparisons for the WHERE clause.
        """
        query = self.build_select(*fields, **where)
        return self.execute_cursor(query, connection=connection, coerce=coerce)

    def build_select(self, *fields: str, **where: Any) -> pypika.queries.QueryBuilder:
        """A convenience method for building a simple SELECT statement.

        Args:
            *fields:
                Optionally specify specific fields to return.
            **where:
                Optionally specify direct equality comparisons for the WHERE clause.
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
        "aiosqlite": pypika.Dialects.SQLLITE,
        "asyncpg": pypika.Dialects.POSTGRESQL,
        "psycopg": pypika.Dialects.POSTGRESQL,
        "sqlite": pypika.Dialects.POSTGRESQL,
    }

    def _do_execute(
        self,
        query: str,
        params: Union[Tuple[Any, ...], Mapping[str, Any]],
        operation: Callable[..., Union[_ReturnT, Awaitable[_ReturnT]]],
        *,
        connection: _ConnT = None,
        coerce: bool = True,
        rtype: Literal["all", "one", "val"] = "all",
    ) -> Union[_ReturnT, Awaitable[_ReturnT]]:
        ...

    def _do_execute_cursor(
        self,
        query: str,
        params: Union[Tuple[Any, ...], Mapping[str, Any]],
        connection: _ConnT = None,
        coerce: bool = True,
    ) -> Union[
        ContextManager[types.SyncCursorProtocolT[_MT]],
        AsyncContextManager[types.AsyncCursorProtocolT[_MT]],
    ]:
        ...

    def _resolve_query(
        self,
        query: Union[str, pypika.queries.QueryBuilder],
        args: Tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> Tuple[str, Union[Tuple[Any, ...], Mapping[str, Any]]]:
        if isinstance(query, pypika.queries.QueryBuilder):
            query = query.get_sql()

        if kwargs and self.service.metadata.__driver__ == "asyncpg":
            warnings.warn(
                f"Driver {self.service.metadata.__driver__!r} "
                "does not accept keyword parameters. "
                "We recommend you pass in arguments for dynamic queries as varargs.",
                stacklevel=5,
            )
            args = (*kwargs.values(),)

        params = args or kwargs
        return query, params


class AsyncDynamicQueryLib(BaseDynamicQueryLib[_MT]):
    """A dynamic query library for asyncio-native drivers."""

    service: service.AsyncQueryService[_MT]

    @support.coerceable(bulk=True)  # type: ignore
    @support.retry
    async def _do_execute(  # type: ignore[override]
        self,
        query: str,
        params: Union[Tuple[Any, ...], Mapping[str, Any]],
        operation: Callable[..., Awaitable[_ReturnT]],
        *,
        connection: _ConnT = None,
        coerce: bool = True,
    ) -> _ReturnT:
        async with self.service.connector.transaction(connection=connection) as c:
            return await operation(c, self._QNAME, sql=query, parameters=params)

    @support.retry
    @contextlib.asynccontextmanager
    async def _do_execute_cursor(
        self,
        query: str,
        params: Union[Tuple[Any, ...], Mapping[str, Any]],
        *,
        connection: _ConnT = None,
        coerce: bool = True,
    ) -> AsyncIterator[types.AsyncCursorProtocolT[_MT]]:
        async with self.service.connector.transaction(connection=connection) as c:
            async with self.service.queries.driver_adapter.select_cursor(
                conn=c, query_name=self._QNAME, sql=query, parameters=params
            ) as factory:
                cursor = await factory
                yield (
                    bootstrap.AsyncCoercingCursor(self.service, cursor)
                    if coerce
                    else cursor
                )


class SyncDynamicQueryLib(BaseDynamicQueryLib[_MT]):
    """A dynamic query library for sync-io-native drivers."""

    service: service.SyncQueryService[_MT]

    @support.coerceable(bulk=True)
    @support.retry
    def _do_execute(  # type: ignore[override]
        self,
        query: str,
        params: Union[Tuple[Any, ...], Mapping[str, Any]],
        operation: Callable[..., _ReturnT],
        *,
        connection: _ConnT = None,
        coerce: bool = True,
    ) -> _ReturnT:
        with self.service.connector.transaction(connection=connection) as c:
            return operation(c, self._QNAME, sql=query, parameters=params)

    @support.retry_cursor
    @contextlib.contextmanager
    def _do_execute_cursor(
        self,
        query: str,
        params: Union[Tuple[Any, ...], Mapping[str, Any]],
        *,
        connection: _ConnT = None,
        coerce: bool = True,
    ) -> Iterator[types.SyncCursorProtocolT[_MT]]:
        with self.service.connector.transaction(connection=connection) as c:
            with self.service.queries.driver_adapter.select_cursor(
                conn=c, query_name=self._QNAME, sql=query, parameters=params
            ) as cursor:
                yield (
                    bootstrap.SyncCoercingCursor(self.service, cursor)
                    if coerce
                    else cursor
                )
