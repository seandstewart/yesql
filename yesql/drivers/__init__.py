from typing import Literal

from . import aio, sio

SupportedDriversT = Literal["aiosqlite", "asyncpg", "psycopg", "sqlite"]
