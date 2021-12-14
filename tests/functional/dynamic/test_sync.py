from __future__ import annotations

import psycopg
import pytest

from examples.pg.db import client
from norma import dynamic
from tests.functional.postgres import factories


@pytest.fixture(scope="module")
def posts() -> client.AsyncPosts:
    return client.SyncPosts()


@pytest.fixture
def session(posts) -> psycopg.Connection:
    with posts.connector.transaction(rollback=True) as c:
        yield c


@pytest.fixture(scope="module")
def queries(posts) -> dynamic.SyncDynamicQueryLib:
    return dynamic.SyncDynamicQueryLib(posts, schema="blog")


def test_execute(queries, session):
    # Given
    batch = factories.PostFactory.create_batch(10)
    queries.service.bulk_create(models=batch, connection=session)
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
    queries.service.bulk_create(models=batch, connection=session)
    created = queries.service.all(connection=session)
    # When
    query = queries.table.select(queries.builder.star).where(
        queries.table.id.isin([c.id for c in created])
    )
    with queries.execute_cursor(query, connection=session) as cursor:
        found = cursor.fetch(len(created))
    # Then
    assert found == created


def test_select_one(queries, session):
    # Given
    created = queries.service.create(
        model=factories.PostFactory.create(), connection=session
    )
    # When
    found = queries.select(connection=session, id=created.id, rtype="one")
    # Then
    assert found == created


def test_select_val(queries, session):
    # Given
    created = queries.service.create(
        model=factories.PostFactory.create(), connection=session
    )
    # When
    id = queries.select("id", title=created.title, rtype="val", connection=session)
    # Then
    assert id == created.id
