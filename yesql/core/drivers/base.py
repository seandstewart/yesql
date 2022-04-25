from __future__ import annotations

import abc
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Iterable,
    Mapping,
    Sequence,
    TypeVar,
    Union,
)

from yesql.core import parse

_T = TypeVar("_T")
_CT = TypeVar("_CT")

__all__ = ("BaseQueryExecutor",)


class BaseQueryExecutor(abc.ABC, Generic[_CT]):
    __driver__: str
    __slots__ = ("pool", "managed", "pool_kwargs", "_lock")

    def __init__(
        self,
        *,
        pool=None,
        **pool_kwargs,
    ):
        self.pool = pool
        self.managed = pool is None
        self.pool_kwargs = pool_kwargs

    def __repr__(self) -> str:
        return (
            "<"
            f"{self.__class__.__name__} "
            f"managed={self.managed}, "
            f"initialized={self.pool is not None}"
            ">"
        )

    @abc.abstractmethod
    def connection(self, *, timeout: float = 10, connection: _CT | None = None):
        ...

    @abc.abstractmethod
    def transaction(
        self,
        *,
        timeout: float = 10,
        connection: _CT | None = None,
        rollback: bool = False,
        **kwargs,
    ):
        ...

    @abc.abstractmethod
    def initialize(self):
        ...

    @abc.abstractmethod
    def teardown(self, *, timeout: int = 10):
        ...

    @abc.abstractmethod
    def many(
        self,
        query: parse.QueryDatum,
        *args,
        connection: _CT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        deserializer: DeserializerT[_T] | None = None,
        **kwargs,
    ):
        ...

    @abc.abstractmethod
    def many_cursor(
        self,
        query: parse.QueryDatum,
        *args,
        connection: _CT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **kwargs,
    ):
        ...

    def raw(
        self,
        query: parse.QueryDatum,
        *args,
        connection: _CT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **kwargs,
    ):
        return self.many(
            query,
            *args,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            **kwargs,
        )

    def raw_cursor(
        self,
        query: parse.QueryDatum,
        *args,
        connection: _CT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **kwargs,
    ):
        return self.many_cursor(
            query,
            *args,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            **kwargs,
        )

    @abc.abstractmethod
    def one(
        self,
        query: parse.QueryDatum,
        *args,
        connection: _CT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        deserializer: DeserializerT[_T] | None = None,
        **kwargs,
    ):
        ...

    @abc.abstractmethod
    def scalar(
        self,
        query: parse.QueryDatum,
        *args,
        connection: _CT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **kwargs,
    ):
        ...

    @abc.abstractmethod
    def multi(
        self,
        query: parse.QueryDatum,
        *,
        params: Iterable[Union[Sequence, Mapping[str, Any]]],
        connection: _CT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        returns: bool = True,
        deserializer: DeserializerT[_T] | None,
    ):
        ...

    @abc.abstractmethod
    def multi_cursor(
        self,
        query: parse.QueryDatum,
        *,
        params: Iterable[Union[Sequence, Mapping[str, Any]]],
        connection: _CT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
    ):
        ...

    @abc.abstractmethod
    def affected(
        self,
        query: parse.QueryDatum,
        *args,
        connection: _CT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **kwargs,
    ):
        ...

    @classmethod
    def get_explain_command(cls, analyze: bool = False, format: str = None) -> str:
        options = (
            f"{'ANALYZE, ' if analyze else ''}"
            f"{'FORMAT ' if format else ''}"
            f"{format or ''}"
        )
        if options:
            return f"{cls.EXPLAIN_PREFIX} ({options})"
        return cls.EXPLAIN_PREFIX

    EXPLAIN_PREFIX = "EXPLAIN"


DeserializerT = Callable[[Any], _T]
SerializerT = Callable[[_T], Collection]
