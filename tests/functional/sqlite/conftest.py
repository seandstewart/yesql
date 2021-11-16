import os
from unittest import mock

import asyncpg
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
    connection: asyncpg.Connection
    connection = sqlite3.connect(dsn)
    cursor = connection.cursor()
    script = db.SCHEMA.read_text()
    cursor.executescript(script)
    yield
    os.unlink(dsn)


@pytest.fixture
def post():
    return factories.PostFactory.create()
