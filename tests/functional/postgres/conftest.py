import os
from unittest import mock

import asyncpg
import pytest

from examples.pg import db

from tests.functional.postgres import factories


@pytest.fixture(scope="package", autouse=True)
def dsn() -> str:
    dsn = "postgres://postgres:@postgres:5432/test?sslmode=disable"
    with mock.patch.dict(os.environ, database_url=dsn):
        yield dsn


@pytest.fixture(scope="package", autouse=True)
async def initdb(dsn):
    connection: asyncpg.Connection
    connection = await asyncpg.connect(dsn)
    try:
        script = db.SCHEMA.read_text()
        await connection.execute(script)
    except asyncpg.DuplicateSchemaError:
        pass
    yield
    await connection.execute(
        "DROP SCHEMA IF EXISTS blog CASCADE; "
        "DROP TABLE IF EXISTS public.schema_migrations CASCADE; "
        "DROP FUNCTION IF EXISTS public.slugify;"
        "DROP FUNCTION IF EXISTS public.get_updated_at;"
    )


@pytest.fixture
def post():
    return factories.PostFactory.create()
