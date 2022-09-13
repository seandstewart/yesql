from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Awaitable, Generic, Iterable, TypeVar, Union

import pypika

from yesql.core import parse, types

if TYPE_CHECKING:
    from yesql import drivers

__all__ = ("DynamicQueryService",)

_MT = TypeVar("_MT")
_RT = TypeVar("_RT")
_ConnT = TypeVar("_ConnT")
_CtxT = TypeVar("_CtxT")
_ReturnT = Union[Iterable[_MT], Iterable[types.ScalarT]]


class DynamicQueryService(Generic[_MT]):
    """Query Library for building ad-hoc queries in-memory.

    This service acts as glue between the `pypika` query-builder library and Norma's
    `QueryRepository`.

    It is intended as an escape-hatch for the small subset of situations where
    dynamically-built queries are actually needed (think an admin tool, like
    Flask-Admin, for instance).
    """

    __slots__ = (
        "service",
        "table",
        "builder",
        "cursor_proxy",
    )

    def __init__(
        self,
        service: types.RepositoryProtocolT[_MT],  # noqa: F811
        *,
        schema: str = None,
    ):
        self.service = service
        self.table = self._get_table(service.metadata.__tablename__, schema=schema)
        self.builder = self._get_query_builder(
            self.table, dialect=service.metadata.__dialect__
        )

    def execute(
        self,
        query: Union[str, pypika.queries.QueryBuilder],
        *args,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        coerce: bool = True,
        deserializer: drivers.base.DeserializerT | None = None,
        modifier: parse.ModifierT = parse.MANY,
        **kwargs,
    ) -> Union[_ReturnT, Awaitable[_ReturnT]]:
        """Execute any arbitrary query and return the result.

        Args:
            query:
               Either a SQL string or a pypika Query.
            *args:
               Any args which should be passed on to the query.
            connection: optional
               A DBAPI connectable to use during executions.
               Whether to coerce the query result into the model bound to the service.
            timeout: defaults 10
                The number of seconds to wait for the query to complete.
            transaction: defaults True
                Whether to execute this query within a transaction block.
            rollback: defaults False
                Whether to rollback the transaction scope of this query execution.
            coerce: defaults True
                Whether to coerce the query result into the model bound to the service.
                Will only be applied if the execution modifier returns a full row object.
            deserializer: optional
                A custom deserializer for the query result. If none provided, this method
                will make use of the default deserializer on the associated repository.
            modifier: The execution modifier for this query.
                Fetch all rows, one row, the first value in the first row,
                or just the affected rows.
            **kwargs:
               Any keyword args to pass on to the query.
        Returns:
            The query result.
        """
        query = self._resolve_query(sql=query, modifier=modifier)
        executor = getattr(self.service.executor, modifier)
        if modifier in {parse.MANY, parse.ONE, parse.MULTI} and coerce:
            deserializer = deserializer or (
                self.service.serdes.deserializer
                if modifier == parse.ONE
                else self.service.serdes.bulk_deserializer
            )
            kwargs.update(deserializer=deserializer)
        return executor(
            query,
            *args,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            **kwargs,
        )

    def execute_cursor(
        self,
        query: Union[str, pypika.queries.QueryBuilder],
        *args,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        modifier: parse.ModifierT = parse.MANY,
        **kwargs,
    ):
        """Execute any arbitrary query and enter a cursor context.

        Args:
            query:
               Either a SQL string or a pypika Query.
            *args:
               Any args which should be passed on to the query.
            connection: optional
               A DBAPI connectable to use during executions.
               Whether to coerce the query result into the model bound to the service.
            timeout: defaults 10
                The number of seconds to wait for the query to complete.
            transaction: defaults True
                Whether to execute this query within a transaction block.
            rollback: defaults False
                Whether to rollback the transaction scope of this query execution.
            modifier: The execution modifier for this query.
                Fetch all rows, one row, the first value in the first row,
                or just the affected rows.
            **kwargs:
               Any keyword args to pass on to the query.
        """
        query = self._resolve_query(sql=query, modifier=modifier)
        executor = getattr(self.service.executor, modifier + "_cursor")
        return executor(
            query,
            *args,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            **kwargs,
        )

    def select(
        self,
        *fields,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        coerce: bool = True,
        deserializer: drivers.base.DeserializerT | None = None,
        modifier: parse.ModifierT = parse.MANY,
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
               Whether to coerce the query result into the model bound to the service.
            timeout: defaults 10
                The number of seconds to wait for the query to complete.
            transaction: defaults True
                Whether to execute this query within a transaction block.
            rollback: defaults False
                Whether to rollback the transaction scope of this query execution.
            coerce: defaults True
                Whether to coerce the query result into the model bound to the service.
                Will only be applied if the execution modifier returns a full row object.
            deserializer: optional
                A custom deserializer for the query result. If none provided, this method
                will make use of the default deserializer on the associated repository.
            modifier: The execution modifier for this query.
                Fetch all rows, one row, the first value in the first row,
                or just the affected rows.
            **where:
                Optionally specify direct equality comparisons for the WHERE clause.
        """
        query = self.build_select(*fields, **where)
        return self.execute(
            query,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            coerce=coerce,
            deserializer=deserializer,
            modifier=modifier,
        )

    def select_cursor(
        self,
        *fields,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        modifier: parse.ModifierT = parse.MANY,
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
               Whether to coerce the query result into the model bound to the service.
            timeout: defaults 10
                The number of seconds to wait for the query to complete.
            transaction: defaults True
                Whether to execute this query within a transaction block.
            rollback: defaults False
                Whether to rollback the transaction scope of this query execution.
            modifier: The execution modifier for this query.
                Fetch all rows, one row, the first value in the first row,
                or just the affected rows.
            **where:
                Optionally specify direct equality comparisons for the WHERE clause.
        """
        query = self.build_select(*fields, **where)
        return self.execute_cursor(
            query,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            modifier=modifier,
        )

    def build_select(self, *fields: str, **where: Any) -> pypika.queries.QueryBuilder:
        """A convenience method for building a simple SELECT statement.

        Args:
            *fields:
                Optionally specify specific fields to return.
            **where:
                Optionally specify direct equality comparisons for the WHERE clause.
        """
        fs: Iterable[str] = fields or [self.builder.star]
        criterion: pypika.Criterion = pypika.Criterion.all(
            [getattr(self.table, c) == v for c, v in where.items()]
        )
        return self.builder.select(*fs).where(criterion)

    @staticmethod
    def _get_table(name: str, *, schema: str = None) -> pypika.Table:
        return pypika.Table(name, schema=schema)

    @classmethod
    def _get_query_builder(
        cls,
        table: pypika.Table,
        *,
        dialect: drivers.SupportedDialectsT = pypika.Dialects.POSTGRESQL,
    ) -> pypika.queries.QueryBuilder:
        return pypika.Query.from_(table, dialect=dialect)

    @staticmethod
    def _resolve_query(
        sql: Union[str, pypika.queries.QueryBuilder], modifier: parse.ModifierT
    ) -> parse.QueryDatum:
        if isinstance(sql, pypika.queries.QueryBuilder):
            sql = sql.get_sql()

        return parse.QueryDatum(
            name="execute",
            doc="",
            sql=sql,
            signature=inspect.Signature(),
            modifier=modifier,
        )
