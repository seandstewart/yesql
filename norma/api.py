# flake8: noqa
from __future__ import annotations

import functools
import pathlib
from typing import Union, Type, Tuple, Any

import inflection
import typic

from .core import drivers, support, types
from .core.service import *
from .core.middleware import *


__all__ = (
    "AsyncQueryService",
    "BaseQueryService",
    "drivers",
    "ExplainFormatT",
    "middleware",
    "QueryMetadata",
    "service",
    "servicemaker",
    "SyncQueryService",
    "support",
    "types",
)


def service(
    model: types.ModelT,
    querylib: pathlib.Path,
    *,
    tablename: str = None,
    driver: drivers.SupportedDriversT = "asyncpg",
    exclude_fields: frozenset[str] = frozenset(),
    scalar_queries: frozenset[str] = frozenset(),
    connector: types.AnyConnectorProtocolT[types.ConnectionT] = None,
    base_service: Type[BaseQueryService[types.ModelT]] = None,
    **connect_kwargs,
) -> Union[AsyncQueryService[types.ModelT], SyncQueryService[types.ModelT]]:
    """Create and instantiate a Query Service object.

    Args:
        model:
            The data model to bind this service to.
        querylib:
            The directory path pointing to your queries.
        tablename: optional
            The name of the table for this query service.
        driver: defaults "asyncpg"
            The client-library for connecting to your database.
        exclude_fields: optional
            Any fields which should be automatically excluded when dumping your model.
        scalar_queries: optional
            Any queries in your library that do not resolve to your model.
        base_service: optional
            Optionally provide your own base class for your query service.
        connector: optional
            An external connector protocol to instantiate your service with.
            (If none is provided, we will determine the correct protocol based upon your driver.)
        **connect_kwargs:
            Any connection parameters to pass to the downstream connector.

    Returns:
        An instance of a new Query Service.
    """
    Service = servicemaker(
        model=model,
        querylib=querylib,
        tablename=tablename,
        driver=driver,
        exclude_fields=exclude_fields,
        scalar_queries=scalar_queries,
        base_service=base_service,  # type: ignore
    )
    return Service(connector=connector, **connect_kwargs)  # type: ignore


@functools.lru_cache(maxsize=None)  # type: ignore
def servicemaker(
    model: types.ModelT,
    querylib: pathlib.Path,
    *,
    tablename: str = None,
    driver: drivers.SupportedDriversT = "asyncpg",
    exclude_fields: frozenset[str] = frozenset(),
    scalar_queries: frozenset[str] = frozenset(),
    base_service: Type[BaseQueryService] = None,
    custom_queries: Tuple[types.QueryMethodProtocolT, ...] = (),
) -> Union[
    Type[AsyncQueryService[types.ModelT]],
    Type[SyncQueryService[types.ModelT]],
]:
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
        driver: defaults "asyncpg"
            The client-library for connecting to your database.
        exclude_fields: optional
            Any fields which should be automatically excluded when dumping your model.
        scalar_queries: optional
            Any queries in your library that do not resolve to your model.
        base_service: optional
            Optionally provide your own base class for your query service.
        custom_queries: optional
            Optionally provide custom implementations of query methods.

    Returns:
        A new query service class.
    """
    BaseService = base_service or (
        SyncQueryService if driver in _SYNC_DRIVERS else AsyncQueryService
    )
    tablename = tablename or typic.get_name(model).lower()  # type: ignore
    Metadata = type(
        f"{tablename.title()}Metadata",
        (BaseService.metadata,),
        {
            "__querylib__": querylib,
            "__tablename__": tablename,
            "__driver__": driver,
            "__exclude_fields__": exclude_fields,
            "__scalar_queries__": scalar_queries,
        },
    )
    namespace: dict[str, Any] = {f.__name__: f for f in custom_queries}
    namespace.update(metadata=Metadata, model=model)
    service_name = inflection.camelize(
        inflection.pluralize(tablename), uppercase_first_letter=True
    ).title()
    Service = type(service_name, (BaseService,), namespace)
    return Service


_SYNC_DRIVERS = {"sqlite", "psycopg"}
