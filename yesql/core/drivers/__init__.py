from __future__ import annotations

from typing import Literal, NamedTuple

from yesql.core.drivers import postgresql
from yesql.core.drivers.base import BaseQueryExecutor


def get_driver(
    *, dialect: SupportedDialectsT = "postgresql", aio: bool = True
) -> Driver:
    """Get the driver configuration for the given dialect and IO protocol.

    Args:
        dialect: The SQL dialect for the driver.
        aio: Whether to use the asyncio-based query executor.

    Raises:
        RuntimeError: If the required SDK is not installed.
    """
    if dialect not in _SUPPORTED_DIALECTS:
        raise RuntimeError(
            f"{dialect!r} is not supported. "
            f"Supported dialects are: {(*_SUPPORTED_DIALECTS,)}."
        )
    if (dialect, aio) not in _DIALECT_AIO_TO_EXECUTOR:
        raise RuntimeError(f"{dialect!r} is not implemented for {aio=}.")
    executor = _DIALECT_AIO_TO_EXECUTOR[(dialect, aio)]
    if executor is NotImplemented:
        drivers = _DIALECT_AIO_TO_DRIVERS[(dialect, aio)]
        raise RuntimeError(f"Required driver(s) {' or '.join(drivers)} not installed.")
    return Driver(executor=executor)


class Driver(NamedTuple):
    executor: type[BaseQueryExecutor]


SupportedDialectsT = Literal["postgresql"]
SupportedDriversT = Literal["asyncpg", "psycopg"]

_SUPPORTED_DIALECTS: set[SupportedDialectsT] = {"postgresql"}
_DIALECT_AIO_TO_EXECUTOR: dict[tuple[SupportedDialectsT, bool], type[BaseQueryExecutor]]
_DIALECT_AIO_TO_EXECUTOR = {
    ("postgresql", True): postgresql.AsyncQueryExecutor,
    ("postgresql", False): postgresql.SyncQueryExecutor,
}
_DIALECT_AIO_TO_DRIVERS: dict[
    tuple[SupportedDialectsT, bool], tuple[SupportedDriversT, ...]
]
_DIALECT_AIO_TO_DRIVERS = {
    ("postgresql", True): ("psycopg", "asyncpg"),
    ("postgresql", False): ("psycopg",),
}
