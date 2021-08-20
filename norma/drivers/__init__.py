# flake8: noqa

from typing import Literal

from norma import protos

try:
    from .aio.sqlite import AIOSQLiteConnector
except (ImportError, ModuleNotFoundError):
    AIOSQLiteConnector = protos.ConnectorProtocol  # type: ignore

try:
    from .aio.pg import AsyncPGConnector
except (ImportError, ModuleNotFoundError):
    AsyncPGConnector = protos.ConnectorProtocol  # type: ignore

SupportedDriversT = Literal["asyncpg", "aiosqlite"]
