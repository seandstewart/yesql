from __future__ import annotations

import dataclasses
import functools
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterable,
    Mapping,
    Sequence,
    TypeVar,
    Union,
    cast,
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
    """A callable representing a single execution context for the associated query.

    A Statement represents a parsed SQL query, the execution of that query, and the
    methodology for serializing parameters and deserializing a response.

    Statement instances are directly callable. The callable signature is determined
    by the :py::meth:`~yesql.uow.Statement.execute` method for each subclass.

    Attributes:
        query: :py::class:`~yesql.core.parse.QueryDatum`
        name: The modifier name for this unit of work.
        executor: :py::class:`~yesql.core.drivers.base.BaseQueryExecutor`.
        serdes: :py::class:`~yesql.uow.SerDes`.
        middleware: A callable which will be passed the executable, query, and parameters
    """

    __slots__ = (
        "query",
        "name",
        "executor",
        "serdes",
        "_middleware",
        "__call__",
    )
    query: parse.QueryDatum
    executor: base.BaseQueryExecutor
    serdes: SerDes[_T]

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
        self.serdes = serdes or cast("SerDes[_T]", generic_serdes())
        self._middleware = middleware
        self.__call__ = self.execute_middleware if middleware else self.execute

    def __repr__(self):
        return (
            f"<{self.__class__.__name__} "
            f"query={self.query.name!r}, "
            f"modifier={self.name!r}, "
            f"middleware={self._middleware and self._middleware.__name__!r}"
            f">"
        )

    @property
    def middleware(self) -> types.MiddlewareMethodProtocolT:
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
        """Execute the associated :py::class:`~yesql.core.parse.QueryDatum`."""
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
        """Execute the associated middleware.

        This will pass the :py::class:`~yesql.uow.Statement` instance to the middleware
        alongside the parameters provided at call-time.
        """
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
        if instance is None:
            return args, kwargs

        serializer = serializer or self.serdes.serializer
        serialized = serializer(instance)
        if isinstance(serialized, Mapping):
            kwargs.update(serialized)
        else:
            args = (
                *args,
                *serialized,
            )

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
    """Execute a query, returning all results as a cursor."""

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
    """Execute a query, returning all results as a list, without deserialization."""

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
    """Execute a query, returning all results as a cursor."""

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
    """Execute a query, returning a single value from the first result."""

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
    """Execute a query with multiple sets of parameters, returning all results."""

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
    """Execute a query with multiple sets of parameters, returning a cursor."""

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


@functools.lru_cache(maxsize=1)
def generic_serdes() -> SerDes[dict]:
    serdes = SerDes(
        serializer=cast("base.SerializerT[dict]", typic.primitive),
        deserializer=cast(
            "base.DeserializerT[dict | None]",
            typic.protocol(dict, is_optional=True).transmute,
        ),
        bulk_deserializer=cast(
            "base.DeserializerT[Iterable[dict]]",
            typic.protocol(Iterable[dict]).transmute,  # type: ignore[misc]
        ),
    )
    return serdes


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
