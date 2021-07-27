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
)

import aiosql.types
import typic

from norma import drivers

ModelT = TypeVar("ModelT")
RawT = TypeVar("RawT", covariant=True, bound=Mapping[str, Any])
ConnectionT = TypeVar("ConnectionT")


@runtime_checkable
class ConnectorProtocol(Protocol[RawT]):
    TRANSIENT: ClassVar[Tuple[BaseException, ...]]
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


@runtime_checkable
class MetadataT(Protocol):
    __slots__ = ()
    __driver__: ClassVar[drivers.SupportedDriversT]
    __tablename__: ClassVar[str]
    __primary_key__: ClassVar[str]
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

    @property
    @abc.abstractmethod
    def pk(self) -> str:
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


class QueryMethodProtocol(Protocol[ModelT, _ReturnT]):
    """The generic signature for a query method on a ServiceProtocol."""

    __name__: str
    __qualname__: str

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
    ) -> _ReturnT:
        ...


class RawProtocolT(QueryMethodProtocol[ModelT, Optional[RawT]]):
    """The signature for a scalar query method on a ServiceProtocol."""


class RawBulkProtocolT(QueryMethodProtocol[ModelT, Iterable[RawT]]):
    """The signature for a bulk scalar query method on a ServiceProtocol."""


class RawPersistProtocolT(QueryMethodProtocol[ModelT, Optional[RawT]]):
    """The signature for a scalar persistence query method on a ServiceProtocol."""

    async def __call__(
        *__,
        model: ModelT = None,
        connection: ConnectionT = None,
        **kwargs,
    ) -> _ReturnT:
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
    ) -> _ReturnT:
        ...


class ModelMethodProtocol(Protocol[_ReturnT], QueryMethodProtocol):
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


class ModelProtocolT(ModelMethodProtocol[ModelReturnT]):
    """The final signature a general query method on a ServiceProtocol"""


class BulkModelProtocolT(ModelMethodProtocol[BulkModelReturnT]):
    """The final query for a bulk query method on a ServiceProtocol"""


class ModelPersistProtocolT(ModelMethodProtocol[ModelReturnT]):
    """The final signature for a persistence query method on a ServiceProtocol"""

    async def __call__(
        _,
        self: ServiceProtocolT[ModelT],
        *__,
        connection: ConnectionT = None,
        coerce: bool = True,
        model: ModelT = None,
        **data,
    ) -> _ReturnT:
        ...


class BulkModelPersistProtocolT(ModelMethodProtocol[Optional[BulkModelReturnT]]):
    """The final signature for a persistence query method on a ServiceProtocol"""

    async def __call__(
        _,
        self: ServiceProtocolT[ModelT],
        *__,
        connection: ConnectionT = None,
        coerce: bool = True,
        data: Iterable[RawT] = (),
        models: Iterable[ModelT] = (),
        **___,
    ) -> _ReturnT:
        ...
