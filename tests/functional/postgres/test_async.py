from __future__ import annotations

import datetime

import asyncpg
import pytest

from examples.pg.db import client, model
from tests.functional.postgres import factories

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def posts() -> client.AsyncPosts:
    return client.AsyncPosts()


@pytest.fixture
async def session(posts) -> asyncpg.Connection:
    async with posts.connector.transaction(rollback=True) as c:
        yield c


async def test_persist(posts, post, session):
    # When
    created: model.Post = await posts.create(model=post, connection=session)
    # Then
    assert created.id
    assert (created.title, created.subtitle, created.tagline, created.body) == (
        post.title,
        post.subtitle,
        post.tagline,
        post.body,
    )


async def test_persist_raw(posts, post, session):
    # When
    created: dict = await posts.create(
        model=post,
        coerce=False,
        connection=session,
    )
    # Then
    assert created["id"]
    assert (
        created["title"],
        created["subtitle"],
        created["tagline"],
        created["body"],
    ) == (post.title, post.subtitle, post.tagline, post.body)


async def test_persist_update(posts, post, session):
    # Given
    created: model.Post = await posts.create(model=post, connection=session)
    # When
    published = await posts.publish(
        id=created.id, publication_date=None, connection=session
    )
    # Then
    assert isinstance(published.publication_date, datetime.date)


async def test_persist_scalar(posts, post, session):
    # Given
    created: model.Post = await posts.create(model=post, connection=session)
    new_tags = {"very", "cool"}
    # When
    tags = await posts.add_tags(id=created.id, tags=new_tags, connection=session)
    # Then
    assert {*tags} == new_tags


async def test_persist_bulk(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    expected = {(post.title, post.subtitle, post.tagline, post.body) for post in batch}
    # When
    await posts.bulk_create(models=batch, connection=session)
    created = await posts.all(connection=session)
    # Then
    assert len(created) == len(batch)
    assert {
        (post.title, post.subtitle, post.tagline, post.body) for post in created
    } == expected


async def test_persist_bulk_raw(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    expected = {(post.title, post.subtitle, post.tagline, post.body) for post in batch}
    # When
    await posts.bulk_create(data=(posts.get_kvs(p) for p in batch), connection=session)
    created = await posts.all(connection=session)
    # Then
    assert len(created) == len(batch)
    assert {
        (post.title, post.subtitle, post.tagline, post.body) for post in created
    } == expected


async def test_cursor_fetch(posts, session):
    # Given
    factories.PostFactory.create_batch(size=10)
    # When
    async with posts.all_cursor(connection=session) as cursor:
        page = await cursor.fetch(n=5)
    # Then
    assert len(page) == 5
    assert all(isinstance(p, model.Post) for p in page)


async def test_cursor_fetch_raw(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    await posts.bulk_create_returning(batch, connection=session)
    # When
    async with posts.all_cursor(connection=session, coerce=False) as cursor:
        page = await cursor.fetch(n=5)
    # Then
    assert len(page) == 5
    assert all(not isinstance(p, model.Post) for p in page)


async def test_cursor_fetchrow(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    await posts.bulk_create_returning(batch, connection=session)
    # When
    async with posts.all_cursor(connection=session) as cursor:
        post = await cursor.fetchrow()
    # Then
    assert isinstance(post, model.Post)


async def test_cursor_fetchrow_raw(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    await posts.bulk_create_returning(batch, connection=session)
    # When
    async with posts.all_cursor(connection=session, coerce=False) as cursor:
        post = await cursor.fetchrow()
    # Then
    assert post and not isinstance(post, model.Post)


async def test_cursor_aiter(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    created = await posts.bulk_create_returning(batch, connection=session)
    # When
    async with posts.all_cursor(connection=session) as cursor:
        fetched = [p async for p in cursor]
    assert fetched == created


async def test_cursor_forward(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    created = await posts.bulk_create_returning(batch, connection=session)
    # When
    async with posts.all_cursor(connection=session) as cursor:
        await cursor.forward(n=len(created))
        post = await cursor.fetchrow()
    # Then
    assert not post


async def test_default(posts, post, session):
    # Given
    post = await posts.create(model=post, connection=session)
    # When
    fetched = await posts.get(id=post.id, connection=session)
    # Then
    assert fetched == post


async def test_default_raw(posts, post, session):
    # Given
    post = await posts.create(model=post, connection=session, coerce=False)
    # When
    fetched = await posts.get(id=post["id"], connection=session, coerce=False)
    # Then
    assert fetched == post
