from __future__ import annotations

import asyncpg
import pytest

from examples.pg.db import client
from norma import dynamic
from tests.functional.postgres import factories

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def posts() -> client.AsyncPosts:
    return client.AsyncPosts()


@pytest.fixture
async def session(posts) -> asyncpg.Connection:
    async with posts.connector.transaction(rollback=True) as c:
        yield c


@pytest.fixture(scope="module")
def queries(posts) -> dynamic.AsyncDynamicQueryLib:
    return dynamic.AsyncDynamicQueryLib(posts, schema="blog")


async def test_execute(queries, session):
    # Given
    batch = factories.PostFactory.create_batch(10)
    created = await queries.service.bulk_create_returning(batch, connection=session)
    # When
    query = queries.table.select(queries.builder.star).where(
        queries.table.id.isin([c.id for c in created])
    )
    found = await queries.execute(query, connection=session)
    # Then
    assert found == created


async def test_execute_cursor(queries, session):
    # Given
    batch = factories.PostFactory.create_batch(10)
    created = await queries.service.bulk_create_returning(batch, connection=session)
    # When
    query = queries.table.select(queries.builder.star).where(
        queries.table.id.isin([c.id for c in created])
    )
    async with queries.execute_cursor(query, connection=session) as cursor:
        found = await cursor.fetch(len(created))
    # Then
    assert found == created


async def test_select_one(queries, session):
    # Given
    created = await queries.service.create(
        model=factories.PostFactory.create(), connection=session
    )
    # When
    found = await queries.select(connection=session, id=created.id, rtype="one")
    # Then
    assert found == created


async def test_select_val(queries, session):
    # Given
    created = await queries.service.create(
        model=factories.PostFactory.create(), connection=session
    )
    # When
    id = await queries.select(
        "id", title=created.title, rtype="val", connection=session
    )
    # Then
    assert id == created.id
