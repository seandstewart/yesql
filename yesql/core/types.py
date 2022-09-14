from __future__ import annotations

import pathlib
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    FrozenSet,
    Generic,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    Union,
)

if TYPE_CHECKING:
    from yesql import statement
    from yesql.core import drivers, parse


ModelT = TypeVar("ModelT")
ScalarT = TypeVar("ScalarT", covariant=True)
ConnectionT = TypeVar("ConnectionT")


class MetadataT:
    __slots__ = ()
    __dialect__: ClassVar[drivers.SupportedDialectsT]
    __tablename__: ClassVar[str]
    __exclude_fields__: ClassVar[FrozenSet[str]]
    __querylib__: ClassVar[Union[str, pathlib.Path]]


class RepositoryProtocolT(Generic[ModelT]):
    # User-defined attributes
    model: ClassVar[ModelT]
    metadata: ClassVar[Type[MetadataT]]
    # Generated attributes
    queries: ClassVar[parse.QueryPackage]
    driver: ClassVar[drivers.Driver]
    # Initialized Attributes
    serdes: statement.SerDes[ModelT]
    executor: drivers.BaseQueryExecutor
    isaio: bool

    TRANSIENT: tuple[type[BaseException], ...]

    def __getattr__(self, item: str) -> statement.StatementsT:
        raise AttributeError(item)

    def count(self, query, *args, connection: Optional[ConnectionT] = None, **kwargs):
        ...

    def explain(self, query, *args, connection: Optional[ConnectionT] = None, **kwargs):
        ...

    def get_kvs(self, model: ModelT) -> Dict[str, Any]:
        ...


class AsyncRepositoryProtocolT(RepositoryProtocolT):
    isaio = True


class SyncRepositoryProtocolT(RepositoryProtocolT):
    isaio = False


_ReturnT = TypeVar("_ReturnT", covariant=True)


class MiddlewareMethodProtocolT(Protocol[_ReturnT]):
    """The final signature for a "middleware" method.

    Middleware methods are an escape-hatch which allows for manual manipulation or
    augmentation of the input and/or return value of the assigned `method`.
    """

    __intercepts__: Tuple[str, ...]

    def __call__(
        self,
        statement: statement.Statement,
        *args,
        connection: ConnectionT | None = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **kwargs,
    ) -> _ReturnT:
        ...


class QueryExecutorMethodT(Protocol[_ReturnT]):
    __name__: str
    __qualname__: str

    def __call__(
        self,
        query: statement.Statement,
        *args,
        connection: ConnectionT | None = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        **kwargs,
    ) -> _ReturnT:
        ...
