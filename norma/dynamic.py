from __future__ import annotations

import contextlib
import inspect
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
    TYPE_CHECKING,
    cast,
)

import pypika
import typic

from norma.core import support, types, bootstrap

if TYPE_CHECKING:
    from norma import drivers
    from norma.core import service  # noqa: F401

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
        "cursor_proxy",
    )

    def __init__(
        self,
        service: types.ServiceProtocolT[_MT],  # noqa: F811
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
        self.cursor_proxy = support.get_cursor_proxy(service.metadata.__driver__)

    def execute(
        self,
        query: Union[str, pypika.queries.QueryBuilder],
        *args,
        connection: _ConnT = None,
        coerce: bool = True,
        rollback: bool = False,
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
            rollback: defaults False
                Whether to rollback the transaction scope of this query execution.
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
        proto: typic.SerdeProtocol[Any] = self.service.bulk_protocol
        if rtype == "one":
            operation = self.service.queries.driver_adapter.select_one
            proto = self.service.protocol
        elif rtype == "val":
            operation = self.service.queries.driver_adapter.select_value
            coerce = False
        return self._do_execute(
            query,
            params,
            operation,
            connection=connection,
            coerce=coerce,
            rollback=rollback,
            proto=proto,
        )

    def execute_cursor(
        self,
        query: Union[str, pypika.queries.QueryBuilder],
        *args,
        connection: _ConnT = None,
        coerce: bool = True,
        rollback: bool = False,
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
            rollback: defaults False
                Whether to rollback the transaction scope of this query execution.
            **kwargs:
                Any keyword args to pass on to the query.
        """
        query, params = self._resolve_query(query, args, kwargs)
        ctx = self._do_execute_cursor(
            query,
            params,
            connection=connection,
            coerce=coerce,
            rollback=rollback,
        )
        return ctx

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
        rollback: bool = False,
        proto: typic.SerdeProtocol = None,
    ) -> Union[_ReturnT, Awaitable[_ReturnT]]:
        ...

    def _do_execute_cursor(
        self,
        query: str,
        params: Union[Tuple[Any, ...], Mapping[str, Any]],
        *,
        connection: _ConnT = None,
        coerce: bool = True,
        rollback: bool = False,
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

    @support.retry  # type: ignore
    async def _do_execute(  # type: ignore[override]
        self,
        query: str,
        params: Union[Tuple[Any, ...], Mapping[str, Any]],
        operation: Callable[..., Awaitable[_ReturnT]],
        *,
        connection: _ConnT = None,
        coerce: bool = True,
        rollback: bool = False,
        proto: typic.SerdeProtocol = None,
    ) -> _ReturnT:
        async with self.service.connector.transaction(
            connection=connection, rollback=rollback
        ) as c:
            result = await operation(c, self._QNAME, sql=query, parameters=params)
            if coerce and result and proto:
                return proto.transmute(result)
            return result

    @support.retry_cursor  # type: ignore
    @contextlib.asynccontextmanager
    async def _do_execute_cursor(
        self,
        query: str,
        params: Union[Tuple[Any, ...], Mapping[str, Any]],
        *,
        connection: _ConnT = None,
        coerce: bool = True,
        rollback: bool = False,
    ) -> AsyncIterator[types.AsyncCursorProtocolT[_MT]]:
        async with self.service.connector.transaction(
            connection=connection, rollback=rollback
        ) as c:
            async with self.service.queries.driver_adapter.select_cursor(
                conn=c, query_name=self._QNAME, sql=query, parameters=params
            ) as factory:
                native = (await factory) if inspect.isawaitable(factory) else factory
                proxy = self.cursor_proxy(native)  # type: ignore
                cursor = (
                    bootstrap.AsyncCoercingCursor(
                        self.service, cast(types.AsyncCursorProtocolT[Mapping], proxy)
                    )
                    if coerce
                    else proxy
                )
                yield cast(types.AsyncCursorProtocolT[_MT], cursor)


class SyncDynamicQueryLib(BaseDynamicQueryLib[_MT]):
    """A dynamic query library for sync-io-native drivers."""

    service: service.SyncQueryService[_MT]

    @support.retry
    def _do_execute(  # type: ignore[override]
        self,
        query: str,
        params: Union[Tuple[Any, ...], Mapping[str, Any]],
        operation: Callable[..., _ReturnT],
        *,
        connection: _ConnT = None,
        coerce: bool = True,
        rollback: bool = False,
        proto: typic.SerdeProtocol = None,
    ) -> _ReturnT:
        with self.service.connector.transaction(
            connection=connection, rollback=rollback
        ) as c:
            result = operation(c, self._QNAME, sql=query, parameters=params)
            if coerce and result and proto:
                return proto.transmute(result)
            return result

    @support.retry_cursor
    @contextlib.contextmanager
    def _do_execute_cursor(
        self,
        query: str,
        params: Union[Tuple[Any, ...], Mapping[str, Any]],
        *,
        connection: _ConnT = None,
        coerce: bool = True,
        rollback: bool = False,
    ) -> Iterator[types.SyncCursorProtocolT[_MT]]:
        with self.service.connector.transaction(
            connection=connection, rollback=rollback
        ) as c:
            with self.service.queries.driver_adapter.select_cursor(
                c, self._QNAME, sql=query, parameters=params
            ) as native:
                proxy = self.cursor_proxy(native)  # type: ignore
                cursor = (
                    bootstrap.SyncCoercingCursor(
                        self.service, cast(types.SyncCursorProtocolT[Mapping], proxy)
                    )
                    if coerce
                    else proxy
                )
                yield cast(types.SyncCursorProtocolT[_MT], cursor)
