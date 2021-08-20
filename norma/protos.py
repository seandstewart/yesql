from __future__ import annotations

import abc
import contextlib
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
    Mapping,
    Type,
    runtime_checkable,
    AsyncIterator,
    Optional,
    AsyncContextManager,
    Dict,
    TYPE_CHECKING,
)

import aiosql.types
import typic

if TYPE_CHECKING:
    from norma import drivers

ModelT = TypeVar("ModelT")
RawT = TypeVar("RawT", covariant=True)
ConnectionT = TypeVar("ConnectionT", covariant=True)


@runtime_checkable
class ConnectorProtocol(Protocol[ConnectionT, RawT]):
    TRANSIENT: ClassVar[Tuple[Type[BaseException], ...]]
    EXPLAIN_PREFIX: str = "EXPLAIN"
    initialized: bool

    @property
    @abc.abstractmethod
    def open(self) -> bool:
        ...

    async def initialize(self):
        ...

    @contextlib.asynccontextmanager
    def connection(
        self, *, timeout: int = 10, c: ConnectionT = None
    ) -> AsyncIterator[ConnectionT]:
        ...

    @contextlib.asynccontextmanager
    def transaction(
        self, *, connection: ConnectionT = None, rollback: bool = False
    ) -> AsyncIterator[ConnectionT]:
        ...

    async def close(self, timeout: int = 10):
        ...

    @classmethod
    def get_explain_command(cls, analyze: bool = False, format: str = None) -> str:
        ...


_T = TypeVar("_T", covariant=True)


class CursorProtocolT(Protocol[_T]):
    async def __aiter__(self) -> _T:
        ...

    async def forward(self, n: int, *args, timeout: float = None, **kwargs):
        ...

    async def fetch(
        self, n: int, *args, timeout: float = None, **kwargs
    ) -> Iterable[_T]:
        ...

    async def fetchrow(self, *args, timeout: float = None, **kwargs) -> Optional[_T]:
        ...


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
    connector: ConnectorProtocol

    def __getattr__(self, item) -> QueryMethodProtocol[ModelT, ModelReturnT]:
        ...

    async def count(
        self, query, *args, connection: ConnectionT = None, **kwargs
    ) -> int:
        ...

    async def explain(
        self, query, *args, connection: ConnectionT = None, **kwargs
    ) -> RawT:
        ...

    def get_kvs(self, model: ModelT) -> Dict[str, Any]:
        ...


_ReturnT = TypeVar("_ReturnT", covariant=True)
_ReturnT_in = TypeVar("_ReturnT_in")


class QueryMethodProtocol(Protocol[ModelT, _ReturnT]):
    """The generic signature for a query method on a ServiceProtocol."""

    __name__: str
    __qualname__: str
    __queryfn__: aiosql.types.QueryFn

    async def __call__(
        _,
        self: ServiceProtocolT[ModelT],
        *args,
        connection: ConnectionT = None,
        **kwargs,
    ) -> _ReturnT:
        ...


_CursorProtocolReturnT = AsyncContextManager[
    Union[CursorProtocolT[ModelT], CursorProtocolT[RawT]]
]


class CursorMethodProtocolT(QueryMethodProtocol[ModelT, _CursorProtocolReturnT]):
    """The final signature for a query cursor method on a ServiceProtocol."""

    async def __call__(
        _,
        self: ServiceProtocolT[ModelT],
        *args,
        connection: ConnectionT = None,
        coerce: bool = True,
        **kwargs,
    ) -> _CursorProtocolReturnT:
        ...


class RawProtocolT(QueryMethodProtocol[ModelT, Optional[RawT]]):
    """The signature for a scalar query method on a ServiceProtocol."""


class RawBulkProtocolT(QueryMethodProtocol[ModelT, Iterable[RawT]]):
    """The signature for a bulk scalar query method on a ServiceProtocol."""


class RawPersistProtocolT(QueryMethodProtocol[ModelT, Optional[RawT]]):
    """The signature for a scalar persistence query method on a ServiceProtocol."""

    async def __call__(
        _,
        self: ServiceProtocolT[ModelT],
        *__,
        connection: ConnectionT = None,
        model: ModelT = None,
        **kwargs,
    ) -> Optional[RawT]:
        ...


class RawBulkPersistProtocolT(QueryMethodProtocol[ModelT, Optional[Iterable[RawT]]]):
    """The signature for a bulk, scalar persistence query method on a ServiceProtocol."""

    async def __call__(
        _,
        self: ServiceProtocolT[ModelT],
        *__,
        connection: ConnectionT = None,
        models: Iterable[ModelT] = (),
        data: Iterable[Mapping] = (),
        **___,
    ) -> Optional[Iterable[RawT]]:
        ...


class ModelMethodProtocol(
    QueryMethodProtocol[ModelT, _ReturnT], Generic[ModelT, _ReturnT]
):
    async def __call__(
        _,
        self: ServiceProtocolT[ModelT],
        *args,
        connection: ConnectionT = None,
        coerce: bool = True,
        **kwargs,
    ) -> _ReturnT:
        ...


ModelReturnT = Union[ModelT, RawT, None]
BulkModelReturnT = Union[Iterable[ModelT], Iterable[RawT]]


class ModelProtocolT(ModelMethodProtocol[ModelT, ModelReturnT]):
    """The final signature a general query method on a ServiceProtocol"""


class BulkModelProtocolT(ModelMethodProtocol[ModelT, BulkModelReturnT]):
    """The final query for a bulk query method on a ServiceProtocol"""


class ModelPersistProtocolT(ModelMethodProtocol[ModelT, ModelReturnT]):
    """The final signature for a persistence query method on a ServiceProtocol"""

    async def __call__(
        _,
        self: ServiceProtocolT[ModelT],
        *__,
        connection: ConnectionT = None,
        coerce: bool = True,
        model: ModelT = None,
        **data,
    ) -> ModelReturnT:
        ...


class BulkModelPersistProtocolT(
    ModelMethodProtocol[ModelT, Optional[BulkModelReturnT]]
):
    """The final signature for a persistence query method on a ServiceProtocol"""

    async def __call__(
        _,
        self: ServiceProtocolT[ModelT],
        *__,
        connection: ConnectionT = None,
        coerce: bool = True,
        models: Iterable[ModelT] = (),
        data: Iterable[RawT] = (),
        **___,
    ) -> BulkModelReturnT:
        ...


class MiddelwareMethodProtocol(Protocol[ModelT, _ReturnT_in]):
    """The final signature for a "finalizer" method.

    Finalizer methods are an escape-hatch which allows for manual manipulation or
    augmentation of the return value of the assigned `__method__`.
    """

    async def __call__(
        _,
        self: ServiceProtocolT[ModelT],
        method: QueryMethodProtocol[ModelT, _ReturnT_in],
        *args,
        connection: ConnectionT = None,
        **kwargs,
    ) -> _ReturnT_in:
        ...
