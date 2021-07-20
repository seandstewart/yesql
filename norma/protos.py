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
)

import aiosql.types
import typic

from norma import drivers

ModelT = TypeVar("ModelT")
RawT = TypeVar("RawT")
ConnectionT = TypeVar("ConnectionT")


@runtime_checkable
class ConnectorProtocol(Protocol[RawT]):
    TRANSIENT: ClassVar[Tuple[BaseException, ...]]
    initialized: bool

    def __init__(self, *args, **kwargs):
        ...

    @property
    @abc.abstractmethod
    def open(self) -> bool:
        ...

    async def initialize(self):
        ...

    @contextlib.asynccontextmanager
    async def connection(
        self, *, timeout: int = 10, c: ConnectionT = None
    ) -> ConnectionT:
        ...

    @contextlib.asynccontextmanager
    async def transaction(self, *, connection: ConnectionT = None) -> ConnectionT:
        ...

    async def close(self, timeout: int = 10):
        ...


_T = TypeVar("_T")


class CursorProtocolT(Protocol[_T]):
    async def __aiter__(self) -> _T:
        ...

    async def forward(self, n: int, *args, timeout: float = None, **kwargs):
        ...

    async def fetch(self, n: int, *args, timeout: float = None, **kwargs):
        ...

    async def fetchrow(self, *args, timeout: float = None, **kwargs):
        ...


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

    def get_kvs(self, model: ModelT) -> Mapping[str, Any]:
        ...
