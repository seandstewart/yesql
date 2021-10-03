from __future__ import annotations

import threading
import contextlib
import contextvars
from typing import Optional, Iterator

import sqlite3
import typic

from norma import types

LOCK: contextvars.ContextVar[Optional[threading.Lock]] = contextvars.ContextVar(
    "sqlite_lock", default=None
)
CONNECTOR: contextvars.ContextVar[Optional[SQLiteConnector]] = contextvars.ContextVar(
    "sqlite_connector", default=None
)


def connnector(**options) -> SQLiteConnector:
    """A high-level connector factory which uses context-local state."""
    with _lock():
        if (connector := CONNECTOR.get()) is None:
            connector = SQLiteConnector(**options)
            CONNECTOR.set(connector)
        connector.initialize()
        return connector


def teardown():
    if (connector := CONNECTOR.get()) is not None:
        connector.close()


class SQLiteConnector(types.SyncConnectorProtocolT[sqlite3.Connection]):
    """A ConnectorProtocol interface for sqlite3."""

    TRANSIENT = (sqlite3.OperationalError,)

    __slots__ = ("options", "initialized")

    def __init__(self, **options):
        self.initialized = False
        self.options = get_options(**options)

    def __repr__(self):
        initialized, open = self.initialized, self.open
        return f"<{self.__class__.__name__} {initialized=} {open=}>"

    def initialize(self):
        if self.initialized:
            return

        with _lock():
            conn: sqlite3.Connection
            with self.connection() as conn:
                cur: sqlite3.Cursor = conn.execute("SELECT 1;")
                cur.close()
            self.initialized = True

    @contextlib.contextmanager
    def connection(
        self, *, timeout: int = 10, c: sqlite3.Connection = None
    ) -> Iterator[sqlite3.Connection]:
        self.initialize()
        if c:
            yield c
        else:
            options = {**self.options}
            options.update(timeout=timeout)
            with sqlite3.connect(**self.options) as conn:
                conn.row_factory = sqlite3.Row
                yield conn
                if conn.in_transaction:
                    conn.rollback()

    @contextlib.contextmanager
    def transaction(
        self, *, connection: sqlite3.Connection = None, rollback: bool = False
    ) -> Iterator[sqlite3.Connection]:
        conn: sqlite3.Connection
        with self.connection(c=connection) as conn:
            yield conn
            if not rollback:
                conn.commit()

    def close(self, timeout: int = 10):
        with _lock():
            self.initialized = False

    @property
    def open(self) -> bool:
        return self.initialized

    @classmethod
    def get_explain_command(cls, analyze: bool = False, format: str = None) -> str:
        return cls.EXPLAIN_PREFIX


@typic.settings(prefix="SQLITE_", aliases={"database_url": "sqlite_database"})
class SQLiteSettings:
    database: Optional[typic.DSN] = None
    timeout: Optional[float] = None
    detect_types: Optional[int] = None
    isolation_level: Optional[str] = None
    check_same_thread: Optional[bool] = None
    cached_statements: Optional[int] = None
    iter_chunk_size: Optional[int] = None


def get_options(**overrides) -> dict:
    settings: SQLiteSettings = SQLiteSettings.transmute(overrides)
    options = {f: v for f, v in settings if v is not None}
    options.setdefault("uri", True)
    return options


def _lock() -> threading.Lock:
    if (lock := LOCK.get()) is None:
        lock = threading.Lock()
        LOCK.set(lock)
    return lock
