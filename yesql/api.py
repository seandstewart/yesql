from __future__ import annotations

import functools
import pathlib
from typing import Any, Tuple, Type

import inflection
import typic

from .core import drivers, parse, support, types
from .core.middleware import *
from .repository import *
from .statement import *

__all__ = (
    "Affected",
    "AsyncQueryRepository",
    "BaseQueryRepository",
    "drivers",
    "ExplainFormatT",
    "Many",
    "ManyCursor",
    "middleware",
    "Multi",
    "MultiCursor",
    "parse",
    "QueryMetadata",
    "Raw",
    "RawCursor",
    "service",
    "servicemaker",
    "Scalar",
    "Statement",
    "statements",
    "SyncQueryRepository",
    "support",
    "types",
)


def service(
    model: types.ModelT,
    querylib: pathlib.Path,
    *,
    tablename: str = None,
    dialect: drivers.SupportedDialectsT = "postgresql",
    isaio: bool = True,
    exclude_fields: frozenset[str] = frozenset(),
    scalar_queries: frozenset[str] = frozenset(),
    executor: drivers.BaseQueryExecutor = None,
    base_service: Type[BaseQueryRepository[types.ModelT]] = None,
    **connect_kwargs,
) -> AsyncQueryRepository[types.ModelT] | SyncQueryRepository[types.ModelT]:
    """Create and instantiate a Query Service object.

    Args:
        model:
            The data model to bind this service to.
        querylib:
            The directory path pointing to your queries.
        tablename: optional
            The name of the table for this query service.
        dialect: defaults "postgresql"
            The SQL Dialect of your database.
        isaio: defaults True
            Whether to use asyncio-based execution or syncio-based.
        exclude_fields: optional
            Any fields which should be automatically excluded when dumping your model.
        scalar_queries: optional
            Any queries in your library that do not resolve to your model.
        base_service: optional
            Optionally provide your own base class for your query service.
        executor: optional
            An externally-managed query executor to attach to the service.
        **connect_kwargs:
            Any connection parameters to pass to the downstream connector.

    Returns:
        An instance of a new Query Service.
    """
    Repository = servicemaker(
        model=model,
        querylib=querylib,
        tablename=tablename,
        dialect=dialect,
        isaio=isaio,
        exclude_fields=exclude_fields,
        scalar_queries=scalar_queries,
        base_service=base_service,  # type: ignore
    )
    return Repository(executor=executor, **connect_kwargs)  # type: ignore


@functools.lru_cache(maxsize=None)
def servicemaker(
    model: types.ModelT,
    querylib: pathlib.Path,
    *,
    tablename: str = None,
    dialect: drivers.SupportedDialectsT = "postgresql",
    isaio: bool = True,
    exclude_fields: frozenset[str] = frozenset(),
    scalar_queries: frozenset[str] = frozenset(),
    base_repository: Type[BaseQueryRepository] = None,
    custom_queries: Tuple[types.QueryExecutorMethodT, ...] = (),
) -> Type[AsyncQueryRepository[types.ModelT]] | Type[SyncQueryRepository[types.ModelT]]:
    """A factory for producing a Query Service class which can be instantiated later.

    Notes:
        This factory caches results based upon the call signature. Multiple calls with
        the same parameters will produce the same class.

    Args:
        model:
            The data model to bind this service to.
        querylib:
            The directory path pointing to your queries.
        tablename: optional
            The name of the table for this query service.
        dialect: defaults "postgresql"
            The SQL dialect for connecting to your database.
        isaio: defaults True
            Whether to use asyncio-based execution or sync-io-based.
        exclude_fields: optional
            Any fields which should be automatically excluded when dumping your model.
        scalar_queries: optional
            Any queries in your library that do not resolve to your model.
        base_repository: optional
            Optionally provide your own base class for your query service.
        custom_queries: optional
            Optionally provide custom implementations of query methods.

    Returns:
        A new query service class.
    """
    BaseRepository = base_repository or (
        AsyncQueryRepository if isaio else SyncQueryRepository
    )
    tablename = tablename or typic.get_name(model).lower()  # type: ignore
    Metadata = type(
        f"{tablename.title()}Metadata",
        (BaseRepository.metadata,),
        {
            "__querylib__": querylib,
            "__tablename__": tablename,
            "__dialect__": dialect,
            "__exclude_fields__": exclude_fields,
            "__scalar_queries__": scalar_queries,
        },
    )
    namespace: dict[str, Any] = {f.__name__: f for f in custom_queries}
    namespace.update(metadata=Metadata, model=model)
    repo_name = inflection.camelize(
        inflection.pluralize(tablename), uppercase_first_letter=True
    ).title()
    Repository = type(repo_name, (BaseRepository,), namespace)
    return Repository
