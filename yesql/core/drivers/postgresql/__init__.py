from __future__ import annotations

from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:

    from ._asyncpg import (
        AsyncPGConnectionSettings,
        AsyncPGPoolSettings,
        AsyncPGQueryExecutor,
    )
    from ._psycopg import (
        AsyncPsycoPGQueryExecutor,
        PsycoPGConnectionSettings,
        PsycoPGPoolSettings,
        PsycoPGQueryExecutor,
        create_sync_pool,
    )

    AsyncQueryExecutor: type[AsyncPGQueryExecutor] | type[AsyncPsycoPGQueryExecutor]
    SyncQueryExecutor: type[PsycoPGQueryExecutor]
    create_async_pool: (
        Callable[..., AsyncPGQueryExecutor] | Callable[..., AsyncPsycoPGQueryExecutor]
    )

else:
    AsyncPGConnectionSettings = NotImplemented
    AsyncPGPoolSettings = NotImplemented
    AsyncQueryExecutor = NotImplemented
    create_async_pool = NotImplemented
    PsycoPGConnectionSettings = NotImplemented
    PsycoPGPoolSettings = NotImplemented
    SyncQueryExecutor = NotImplemented
    create_sync_pool = NotImplemented

    try:
        from ._asyncpg import AsyncPGConnectionSettings, AsyncPGPoolSettings
        from ._asyncpg import AsyncPGQueryExecutor as AsyncQueryExecutor
        from ._asyncpg import create_pool as create_async_pool
    except (ImportError, ModuleNotFoundError):
        pass

    try:
        if AsyncQueryExecutor is NotImplemented:
            from ._psycopg import (
                AsyncPsycoPGQueryExecutor as AsyncQueryExecutor,
                create_async_pool,
            )
        from ._psycopg import PsycoPGConnectionSettings, PsycoPGPoolSettings
        from ._psycopg import PsycoPGQueryExecutor as SyncQueryExecutor
        from ._psycopg import create_sync_pool
    except (ImportError, ModuleNotFoundError):
        pass


__all__ = (
    "AsyncPGConnectionSettings",
    "AsyncPGPoolSettings",
    "AsyncQueryExecutor",
    "create_async_pool",
    "create_sync_pool",
    "PsycoPGConnectionSettings",
    "PsycoPGPoolSettings",
    "SyncQueryExecutor",
)
