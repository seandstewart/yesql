from __future__ import annotations

import abc
import pathlib
from typing import (
    Protocol,
    TypeVar,
    Iterable,
    Tuple,
    Generic,
    Any,
    FrozenSet,
    ClassVar,
    Union,
    Type,
    Optional,
    AsyncContextManager,
    Dict,
    NoReturn,
    Awaitable,
    ContextManager,
    Container,
    AsyncIterable,
    TYPE_CHECKING,
)

import aiosql.types
import typic

if TYPE_CHECKING:
    from . import drivers


ModelT = TypeVar("ModelT")
ScalarT = TypeVar("ScalarT", covariant=True)
ConnectionT = TypeVar("ConnectionT")
_CtxT = TypeVar("_CtxT", covariant=True)
_NoReturnT = TypeVar("_NoReturnT", covariant=True)
_ConnT = TypeVar("_ConnT", contravariant=True)


class ConnectorProtocol(Protocol[_CtxT, _NoReturnT, _ConnT]):
    TRANSIENT: ClassVar[Tuple[Type[BaseException], ...]]
    EXPLAIN_PREFIX: str = "EXPLAIN"
    initialized: bool

    @property
    @abc.abstractmethod
    def open(self) -> bool:
        ...

    def initialize(self) -> _NoReturnT:
        ...

    def connection(self, *, timeout: int = 10, c: _ConnT = None) -> _CtxT:
        ...

    def transaction(
        self, *, connection: _ConnT = None, rollback: bool = False
    ) -> _CtxT:
        ...

    def close(self, timeout: int = 10) -> _NoReturnT:
        ...

    @classmethod
    def get_explain_command(cls, analyze: bool = False, format: str = None) -> str:
        ...


SyncConnectorProtocolT = ConnectorProtocol[
    ContextManager[ConnectionT], NoReturn, ConnectionT
]
AsyncConnectorProtocolT = ConnectorProtocol[
    AsyncContextManager[ConnectionT], Awaitable[NoReturn], ConnectionT
]
AnyConnectorProtocolT = Union[
    SyncConnectorProtocolT[ConnectionT], AsyncConnectorProtocolT[ConnectionT]
]


_T = TypeVar("_T", covariant=True)
_IterRt = TypeVar("_IterRt", covariant=True)
_SingleRt = TypeVar("_SingleRt", covariant=True)


class _BaseCursorProtocolT(Protocol[_IterRt, _SingleRt]):
    def forward(self, n: int, *args, timeout: float = None, **kwargs):
        ...

    def fetch(self, n: int, *args, timeout: float = None, **kwargs) -> _IterRt:
        ...

    def fetchrow(self, *args, timeout: float = None, **kwargs) -> _SingleRt:
        ...


class AsyncCursorProtocolT(
    AsyncIterable[_T],
    _BaseCursorProtocolT[Awaitable[Iterable[_T]], Awaitable[Optional[_T]]],
):
    ...


class SyncCursorProtocolT(
    Iterable[_T], _BaseCursorProtocolT[Iterable[_T], Optional[_T]]
):
    ...


CursorProtocolT = Union[AsyncCursorProtocolT[_T], SyncCursorProtocolT[_T]]


class MetadataT:
    __slots__ = ()
    __driver__: ClassVar[drivers.SupportedDriversT]
    __tablename__: ClassVar[str]
    __exclude_fields__: ClassVar[FrozenSet[str]]
    __scalar_queries__: ClassVar[FrozenSet[str]]
    __querylib__: ClassVar[Union[str, pathlib.Path]]


class ServiceProtocolT(Generic[ModelT]):
    # User-defined attributes
    model: ClassVar[ModelT]
    metadata: ClassVar[Type[MetadataT]]
    # Generated attributes
    protocol: ClassVar[typic.SerdeProtocol[ModelT]]
    bulk_protocol: ClassVar[typic.SerdeProtocol[Iterable[ModelT]]]
    queries: ClassVar[aiosql.aiosql.Queries]
    # Initialized Attributes
    connector: AnyConnectorProtocolT

    def __getattr__(self, item: str) -> QueryMethodProtocolT[ModelT]:
        ...

    def count(self, query, *args, connection: Optional[ConnectionT] = None, **kwargs):
        ...

    def explain(self, query, *args, connection: Optional[ConnectionT] = None, **kwargs):
        ...

    def get_kvs(self, model: ModelT) -> Dict[str, Any]:
        ...


class AsyncServiceProtocolT(ServiceProtocolT):
    connector: AsyncConnectorProtocolT


class SyncServiceProtocolT(ServiceProtocolT):
    connector: SyncConnectorProtocolT


_ReturnT = TypeVar("_ReturnT", covariant=True)
_ReturnT_in = TypeVar("_ReturnT_in")


class QueryMethodProtocolT(Protocol[_ReturnT]):
    """The generic signature for a query method on a ServiceProtocol."""

    __name__: str
    __qualname__: str
    __queryfn__: aiosql.types.QueryFn

    def __call__(
        _,
        self: ServiceProtocolT,
        *args,
        connection: Optional[ConnectionT] = None,
        **kwargs,
    ) -> _ReturnT:
        ...


class CursorMethodProtocolT(QueryMethodProtocolT[CursorProtocolT[_ReturnT]]):
    """The final signature for a query cursor method on a ServiceProtocol."""

    def __call__(
        _,
        self: ServiceProtocolT,
        *args,
        connection: Optional[ConnectionT] = None,
        coerce: bool = True,
        **kwargs,
    ) -> CursorProtocolT[_ReturnT]:
        ...


ScalarBulkMethodProtocolT = QueryMethodProtocolT[Iterable[_ReturnT]]


class ScalarPersistProtocolT(QueryMethodProtocolT[_ReturnT]):
    """The signature for a scalar persistence query method on a ServiceProtocol."""

    def __call__(
        _,
        self: ServiceProtocolT,
        *__,
        connection: Optional[ConnectionT] = None,
        model: Optional[ModelT] = None,
        **kwargs,
    ) -> _ReturnT:
        ...


class ScalarBulkPersistProtocolT(QueryMethodProtocolT[_ReturnT]):
    """The signature for a bulk, scalar persistence query method on a ServiceProtocol."""

    def __call__(
        _,
        self: ServiceProtocolT,
        *__,
        connection: Optional[ConnectionT] = None,
        data: Iterable[Container] = (),
        **___,
    ) -> _ReturnT:
        ...


class ModelMethodProtocolT(QueryMethodProtocolT[_ReturnT]):
    def __call__(
        _,
        self: ServiceProtocolT,
        *args,
        connection: Optional[ConnectionT] = None,
        coerce: bool = True,
        **kwargs,
    ) -> _ReturnT:
        ...


ModelBulkMethodProtocolT = ModelMethodProtocolT[Iterable[_ReturnT]]


class ModelPersistProtocolT(ModelMethodProtocolT[_ReturnT]):
    """The final signature for a persistence query method on a ServiceProtocol"""

    def __call__(
        _,
        self: ServiceProtocolT,
        *__,
        connection: Optional[ConnectionT] = None,
        coerce: bool = True,
        model: Optional[ModelT] = None,
        **data,
    ) -> _ReturnT:
        ...


class ModelBulkPersistProtocolT(ModelMethodProtocolT[_ReturnT]):
    """The final signature for a persistence query method on a ServiceProtocol"""

    def __call__(
        _,
        self: ServiceProtocolT,
        *__,
        connection: Optional[ConnectionT] = None,
        coerce: bool = True,
        models: Iterable[ModelT] = (),
        data: Iterable[Container] = (),
        **___,
    ) -> _ReturnT:
        ...


class MiddelwareMethodProtocolT(Protocol[_ReturnT_in]):
    """The final signature for a "middleware" method.

    Middleware methods are an escape-hatch which allows for manual manipulation or
    augmentation of the input and/or return value of the assigned `method`.
    """

    __intercepts__: Tuple[str, ...]

    def __call__(
        _,
        self: ServiceProtocolT,
        method: QueryMethodProtocolT[_ReturnT_in],
        *args,
        connection: Optional[ConnectionT] = None,
        **kwargs,
    ) -> _ReturnT_in:
        ...
