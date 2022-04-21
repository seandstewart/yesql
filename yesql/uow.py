from __future__ import annotations

import dataclasses
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterable,
    Mapping,
    Sequence,
    TypeVar,
    Union,
)

import typic

from yesql.core import parse

if TYPE_CHECKING:
    from yesql.core import types
    from yesql.core.drivers import base


__all__ = (
    "statements",
    "Statement",
    "Affected",
    "Many",
    "ManyCursor",
    "Multi",
    "MultiCursor",
    "One",
    "Raw",
    "RawCursor",
    "Scalar",
)


def statements(
    query: parse.QueryDatum,
    *,
    executor: base.BaseQueryExecutor = None,
    middleware: types.MiddlewareMethodProtocolT = None,
    serdes: SerDes[_T] = None,
) -> Iterable[Statement[_T]]:
    """Get the statements for a given QueryDatum.

    Queries which may return multiple results or perform multiple executions will have a
    standard statement and a cursor statement.
    """
    return [
        cls(query=query, executor=executor, middleware=middleware, serdes=serdes)
        for cls in _MODIFIER_TO_STATEMENTS[query.modifier]
    ]


_T = TypeVar("_T")


SentinelType = type("NoneType")
Sentinel = SentinelType()


class Statement(Generic[_T]):
    """A single Unit of Work for executing a query and interpreting the result.

    A Statement represents a parsed SQL query, the execution of that query, and the
    methodology for deserializing a response.

    Statement instances are directly callable. The callable signature is determined
    by the :py::meth:`~yesql.uow.Statement.execute` method for each subclass.

    Attributes:
        query: :py::class:`~yesql.core.parse.QueryDatum`
        name: The modifier name for this unit of work.
        executor: :py::class:`~yesql.core.drivers.base.BaseQueryExecutor`.
        serdes: :py::class:`~yesql.uow.SerDes`.
    """

    __slots__ = (
        "query",
        "name",
        "executor",
        "serdes",
        "_middleware",
        "__call__",
    )

    def __init__(
        self,
        *,
        query: parse.QueryDatum,
        executor: base.BaseQueryExecutor = None,
        middleware: types.MiddlewareMethodProtocolT = None,
        serdes: SerDes[_T] = None,
    ):
        self.query = query
        self.name = query.modifier
        self.executor = executor
        self.serdes = serdes
        self._middleware = middleware
        self.__call__ = self.execute_middleware if middleware else self.execute

    def __repr__(self):
        return (
            f"<{self.__class__.__name__} "
            f"query={self.query.name!r}, "
            f"modifier={self.name!r}>"
        )

    @property
    def middleware(self):
        return self._middleware

    @middleware.setter
    def middleware(self, m: types.MiddlewareMethodProtocolT | None):
        self._middleware = m
        self.__call__ = self.execute_middleware if m else self.execute

    @middleware.deleter
    def middleware(self):
        self._middleware = None
        self.__call__ = self.execute

    def execute(
        self,
        *args,
        instance: _T = None,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        serializer: base.SerializerT | None = None,
        **kwargs,
    ):
        raise NotImplementedError()

    def execute_middleware(
        self,
        *args,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        deserializer: base.DeserializerT | None = None,
        **kwargs,
    ):
        deserializer = deserializer or self.serdes.bulk_deserializer
        if deserializer:
            kwargs["deserializer"] = deserializer

        return self._middleware(
            self,
            *args,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            **kwargs,
        )

    def _serialize_instance(
        self,
        *,
        instance: _T,
        serializer: base.SerializerT | None,
        args: Sequence,
        kwargs: dict,
    ) -> tuple[Sequence, dict]:
        if instance is not None:
            serializer = serializer or self.serdes.serializer
            serialized = serializer(instance)
            if isinstance(serialized, Mapping):
                kwargs.update(serialized)
            else:
                args = [*args, *serialized]

        return args, kwargs

    def _serialize_instances(
        self,
        *,
        instances: Iterable[_T] = (),
        params: Iterable,
        serializer: base.SerializerT | None,
    ):
        if not instances:
            return params
        serializer = serializer or self.serdes.serializer
        serialized = [*params, *(serializer(i) for i in instances)]
        return serialized


class StatementCursor(Statement[_T]):
    """The StatementCursor provides direct access to a query result cursor."""

    def __init__(
        self,
        *,
        query: parse.QueryDatum,
        executor: base.BaseQueryExecutor = None,
        middleware: types.MiddlewareMethodProtocolT = None,
        serdes: SerDes[_T] = None,
    ):
        query = self._cursor_datum(query)
        super().__init__(
            query=query, executor=executor, middleware=middleware, serdes=serdes
        )

    @typic.fastcachedmethod
    def _cursor_datum(self, query: parse.QueryDatum) -> parse.QueryDatum:
        if query.name.endswith("_cursor"):
            return query
        return dataclasses.replace(query, name=query.name + "_cursor")


class Many(Statement[_T]):
    """Execute a query, returning all results as a list."""

    def execute(
        self,
        *args,
        instance: _T = None,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        serializer: base.SerializerT | None = None,
        coerce: bool = True,
        deserializer: base.DeserializerT | None = None,
        **kwargs,
    ):
        sargs, skwargs = self._serialize_instance(
            instance=instance, serializer=serializer, args=args, kwargs=kwargs
        )
        deserializer = deserializer or self.serdes.bulk_deserializer
        return self.executor.many(
            self.query,
            *sargs,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            deserializer=deserializer if coerce else None,
            **skwargs,
        )


class ManyCursor(StatementCursor):
    def execute(
        self,
        *args,
        instance: _T = None,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        serializer: base.SerializerT | None = None,
        **kwargs,
    ):
        sargs, skwargs = self._serialize_instance(
            instance=instance, serializer=serializer, args=args, kwargs=kwargs
        )
        return self.executor.many_cursor(
            self.query,
            *sargs,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            **skwargs,
        )


class Raw(Statement):
    def execute(
        self,
        *args,
        instance: _T = None,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        serializer: base.SerializerT | None = None,
        **kwargs,
    ):
        sargs, skwargs = self._serialize_instance(
            instance=instance, serializer=serializer, args=args, kwargs=kwargs
        )
        return self.executor.raw(
            self.query,
            *sargs,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            **skwargs,
        )


class RawCursor(StatementCursor):
    def execute(
        self,
        *args,
        instance: _T = None,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        serializer: base.SerializerT | None = None,
        **kwargs,
    ):
        sargs, skwargs = self._serialize_instance(
            instance=instance, serializer=serializer, args=args, kwargs=kwargs
        )
        return self.executor.raw_cursor(
            self.query,
            *sargs,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            **skwargs,
        )


class One(Statement):
    def execute(
        self,
        *args,
        instance: _T = None,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        serializer: base.SerializerT = None,
        coerce: bool = True,
        deserializer: base.DeserializerT | None = None,
        **kwargs,
    ):
        sargs, skwargs = self._serialize_instance(
            instance=instance, serializer=serializer, args=args, kwargs=kwargs
        )
        deserializer = deserializer or self.serdes.deserializer
        return self.executor.one(
            self.query,
            *sargs,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            deserializer=deserializer if coerce else None,
            **skwargs,
        )


class Scalar(Statement):
    def execute(
        self,
        *args,
        instance: _T = None,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        serializer: base.SerializerT | None = None,
        **kwargs,
    ):
        sargs, skwargs = self._serialize_instance(
            instance=instance, serializer=serializer, args=args, kwargs=kwargs
        )
        return self.executor.scalar(
            self.query,
            *sargs,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            **skwargs,
        )


class Multi(Statement):
    def execute(  # type: ignore[override]
        self,
        *,
        instances: Iterable[_T] = (),
        params: Iterable[Sequence | Mapping[str, Any]] = (),
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        coerce: bool = True,
        returns: bool = False,
        deserializer: base.DeserializerT | None = None,
        serializer: base.SerializerT | None = None,
    ):
        params = self._serialize_instances(
            instances=instances, params=params, serializer=serializer
        )
        deserializer = deserializer or self.serdes.bulk_deserializer
        return self.executor.multi(
            self.query,
            params=params,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            returns=returns,
            deserializer=deserializer if coerce else None,
        )


class MultiCursor(StatementCursor):
    def execute(  # type: ignore[override]
        self,
        *,
        instances: Iterable[_T] = (),
        params: Iterable[Sequence | Mapping[str, Any]] = (),
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        serializer: base.SerializerT | None = None,
        **_,
    ):
        params = self._serialize_instances(
            instances=instances, params=params, serializer=serializer
        )
        return self.executor.multi_cursor(
            self.query,
            params=params,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
        )


class Affected(Statement):
    def execute(
        self,
        *args,
        instance: _T = None,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        serializer: base.SerializerT | None = None,
        **kwargs,
    ):
        sargs, skwargs = self._serialize_instance(
            instance=instance,
            serializer=serializer,
            args=args,
            kwargs=kwargs,
        )
        return self.executor.affected(
            self.query,
            *sargs,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            **skwargs,
        )


@typic.slotted(dict=False, weakref=True)
@dataclasses.dataclass
class SerDes(Generic[_T]):
    """A container for Serializer and Deserializers bound to a specifc type.

    This is used by the :py::class:`~yesql.uow.Statement` to serialize model instances
    for query execution and also deserialize query responses.
    """

    serializer: base.SerializerT[_T]
    deserializer: base.DeserializerT[_T | None]
    bulk_deserializer: base.DeserializerT[Iterable[_T]]


StatementsT = Union[
    Affected,
    Many,
    ManyCursor,
    Multi,
    MultiCursor,
    One,
    Raw,
    RawCursor,
    Scalar,
]
_MODIFIER_TO_STATEMENTS: dict[parse.ModifierT, list[type[StatementsT]]] = {
    parse.AFFECTED: [Affected],
    parse.MANY: [Many, ManyCursor],
    parse.MULTI: [Multi, MultiCursor],
    parse.ONE: [One],
    parse.RAW: [Raw, RawCursor],
    parse.SCALAR: [Scalar],
}
