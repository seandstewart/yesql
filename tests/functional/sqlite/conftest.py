import os
from unittest import mock

import sqlite3
import pytest

from examples.sqlite import db

from tests.functional.postgres import factories


@pytest.fixture(scope="package", autouse=True)
def dsn() -> str:
    dsn = "test.db"
    with mock.patch.dict(os.environ, database_url=dsn):
        yield dsn


@pytest.fixture(scope="package", autouse=True)
def initdb(dsn):
    connection: sqlite3.Connection
    connection = sqlite3.connect(dsn)
    cursor = connection.cursor()
    script = db.SCHEMA.read_text()
    cursor.executescript(script)
    yield
    os.unlink(dsn)


@pytest.fixture
def post():
    return factories.PostFactory.create()


MIN_SQLITE_RETURNING = (3, 35, 0)
xfail_sqlite_unsupported = pytest.mark.xfail(
    sqlite3.sqlite_version_info < MIN_SQLITE_RETURNING,
    reason=f"RETURNING clauses are unsupported in SQlite v{sqlite3.sqlite_version}.",
)
