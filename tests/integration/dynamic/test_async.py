from __future__ import annotations

import asyncpg
import pytest

from examples.pg.db import client
from tests.integration.repository.postgresql import factories
from yesql import dynamic

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
async def posts() -> client.AsyncPosts:
    async with client.AsyncPosts() as posts:
        yield posts


@pytest.fixture
async def session(posts) -> asyncpg.Connection:
    async with posts.executor.transaction(rollback=True) as c:
        yield c


@pytest.fixture(scope="module")
def queries(posts) -> dynamic.DynamicQueryService:
    return dynamic.DynamicQueryService(posts, schema="blog")


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
        response = await cursor.fetch(len(created))
        found = queries.service.serdes.bulk_deserializer(response)
    # Then
    assert found == created


async def test_select_one(queries, session):
    # Given
    created = await queries.service.create(
        instance=factories.PostFactory.create(), connection=session
    )
    # When
    found = await queries.select(connection=session, id=created.id, modifier="one")
    # Then
    assert found == created


async def test_select_scalar(queries, session):
    # Given
    created = await queries.service.create(
        instance=factories.PostFactory.create(), connection=session
    )
    # When
    id = await queries.select(
        "id", title=created.title, connection=session, modifier="scalar"
    )
    # Then
    assert id == created.id
