# flake8: noqa

from typing import Literal

from . import asyncpg, aiosqlite

SupportedDriversT = Literal["asyncpg", "aiosqlite"]
