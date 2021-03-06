from __future__ import annotations

import datetime

import asyncpg
import pytest

from examples.pg.db import client, model
from tests.integration.repository.postgresql import factories

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
async def posts() -> client.AsyncPosts:
    async with client.AsyncPosts() as posts:
        yield posts


@pytest.fixture
async def session(posts) -> asyncpg.Connection:
    async with posts.executor.transaction(rollback=True) as c:
        yield c


async def test_persist(posts, post, session):
    # When
    created: model.Post = await posts.create(instance=post, connection=session)
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
        instance=post,
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
    created: model.Post = await posts.create(instance=post, connection=session)
    # When
    published = await posts.publish(
        id=created.id, publication_date=None, connection=session
    )
    # Then
    assert isinstance(published.publication_date, datetime.date)


async def test_persist_scalar(posts, post, session):
    # Given
    created: model.Post = await posts.create(instance=post, connection=session)
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
    await posts.bulk_create(instances=batch, connection=session)
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
    await posts.bulk_create(
        params=(posts.get_kvs(p) for p in batch), connection=session
    )
    created = await posts.all(connection=session)
    # Then
    assert len(created) == len(batch)
    assert {
        (post.title, post.subtitle, post.tagline, post.body) for post in created
    } == expected


async def test_cursor(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    await posts.bulk_create(instances=batch, connection=session)
    # When
    async with posts.all_cursor(connection=session) as cursor:
        page = await cursor.fetch(n=5)
    # Then
    assert len(page) == 5
    assert all(isinstance(p, asyncpg.Record) for p in page)


async def test_bulk_create_returning(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    expected = {(post.title, post.subtitle, post.tagline, post.body) for post in batch}
    # When
    created = await posts.bulk_create_returning(batch, connection=session)
    # Then
    assert {
        (post.title, post.subtitle, post.tagline, post.body) for post in created
    } == expected


async def test_default(posts, post, session):
    # Given
    post = await posts.create(instance=post, connection=session)
    # When
    fetched = await posts.get(id=post.id, connection=session)
    # Then
    assert fetched == post


async def test_default_raw(posts, post, session):
    # Given
    post = await posts.create(instance=post, connection=session, coerce=False)
    # When
    fetched = await posts.get(id=post["id"], connection=session, coerce=False)
    # Then
    assert fetched == post


async def test_default_mutate_scalar(posts, post, session):
    # Given
    post = await posts.create(instance=post, connection=session)
    new_tags = {"click", "here"}
    # When
    all_tags = await posts.add_tags(id=post.id, tags=new_tags, connection=session)
    # Then
    assert set(all_tags) & new_tags == new_tags
