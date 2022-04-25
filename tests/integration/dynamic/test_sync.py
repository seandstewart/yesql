from __future__ import annotations

import psycopg
import pytest

from examples.pg.db import client
from tests.integration.repository.postgresql import factories
from yesql import dynamic


@pytest.fixture(scope="module")
def posts() -> client.SyncPosts:
    with client.SyncPosts(min_size=2) as posts:
        yield posts


@pytest.fixture
def session(posts) -> psycopg.Connection:
    with posts.executor.transaction(rollback=True) as c:
        yield c


@pytest.fixture(scope="module")
def queries(posts) -> dynamic.DynamicQueryService:
    return dynamic.DynamicQueryService(posts, schema="blog")


def test_execute(queries, session):
    # Given
    batch = factories.PostFactory.create_batch(10)
    queries.service.bulk_create(instances=batch, connection=session)
    created = queries.service.all(connection=session)
    # When
    query = queries.table.select(queries.builder.star).where(
        queries.table.id.isin([c.id for c in created])
    )
    found = queries.execute(query, connection=session)
    # Then
    assert found == created


def test_execute_cursor(queries, session):
    # Given
    batch = factories.PostFactory.create_batch(10)
    queries.service.bulk_create(instances=batch, connection=session)
    created = sorted(queries.service.all(connection=session), key=lambda x: x.id)
    # When
    query = queries.table.select(queries.builder.star).where(
        queries.table.id.isin([c.id for c in created])
    )
    cursor: psycopg.Cursor
    with queries.execute_cursor(query, connection=session) as cursor:
        response = cursor.fetchall()
        found = sorted(
            queries.service.serdes.bulk_deserializer(response), key=lambda x: x.id
        )
    # Then
    assert found == created


def test_select_one(queries, session):
    # Given
    created = queries.service.create(
        instance=factories.PostFactory.create(), connection=session
    )
    # When
    found = queries.select(connection=session, id=created.id, modifier="one")
    # Then
    assert found == created


def test_select_scalar(queries, session):
    # Given
    created = queries.service.create(
        instance=factories.PostFactory.create(), connection=session
    )
    # When
    id = queries.select(
        "id", title=created.title, modifier="scalar", connection=session
    )
    # Then
    assert id == created.id
