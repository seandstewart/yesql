# flake8: noqa

from typing import Literal

from norma import protos

try:
    from .aiosqlite import AIOSQLiteConnector
except (ImportError, ModuleNotFoundError):
    AIOSQLiteConnector = protos.ConnectorProtocol  # type: ignore

try:
    from .asyncpg import AsyncPGConnector
except (ImportError, ModuleNotFoundError):
    AsyncPGConnector = protos.ConnectorProtocol  # type: ignore

SupportedDriversT = Literal["asyncpg", "aiosqlite"]
