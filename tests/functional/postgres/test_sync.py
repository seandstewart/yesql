from __future__ import annotations

import datetime

import psycopg
import pytest

from examples.pg.db import client, model
from tests.functional.postgres import factories


@pytest.fixture(scope="module")
def posts() -> client.SyncPosts:
    return client.SyncPosts()


@pytest.fixture
def session(posts) -> psycopg.Connection:
    with posts.connector.transaction(rollback=True) as c:
        yield c


def test_persist(posts, post, session):
    # When
    created: model.Post = posts.create(model=post, connection=session)
    # Then
    assert created.id
    assert (created.title, created.subtitle, created.tagline, created.body) == (
        post.title,
        post.subtitle,
        post.tagline,
        post.body,
    )


def test_persist_raw(posts, post, session):
    # When
    created = posts.create(
        model=post,
        coerce=False,
        connection=session,
    )
    # Then
    assert created.id
    assert (created.title, created.subtitle, created.tagline, created.body) == (
        post.title,
        post.subtitle,
        post.tagline,
        post.body,
    )


def test_persist_update(posts, post, session):
    # Given
    created: model.Post = posts.create(model=post, connection=session)
    # When
    published = posts.publish(id=created.id, publication_date=None, connection=session)
    # Then
    assert isinstance(published.publication_date, datetime.date)


def test_persist_scalar(posts, post, session):
    # Given
    created: model.Post = posts.create(model=post, connection=session)
    new_tags = {"very", "cool"}
    # When
    tags = posts.add_tags(id=created.id, tags=new_tags, connection=session)
    # Then
    assert {*tags} == new_tags


def test_persist_bulk(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    expected = {(post.title, post.subtitle, post.tagline, post.body) for post in batch}
    # When
    posts.bulk_create(models=batch, connection=session)
    created = posts.all(connection=session)
    # Then
    assert len(created) == len(batch)
    assert {
        (post.title, post.subtitle, post.tagline, post.body) for post in created
    } == expected


def test_persist_bulk_raw(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    expected = {(post.title, post.subtitle, post.tagline, post.body) for post in batch}
    # When
    posts.bulk_create(data=(posts.get_kvs(p) for p in batch), connection=session)
    created = posts.all(connection=session)
    # Then
    assert len(created) == len(batch)
    assert {
        (post.title, post.subtitle, post.tagline, post.body) for post in created
    } == expected


def test_cursor_fetch(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    posts.bulk_create(batch, connection=session)
    # When
    with posts.all_cursor(connection=session) as cursor:
        page = cursor.fetch(n=5)
    # Then
    assert len(page) == 5
    assert all(isinstance(p, model.Post) for p in page)


def test_cursor_fetch_raw(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    posts.bulk_create(batch, connection=session)
    # When
    with posts.all_cursor(connection=session, coerce=False) as cursor:
        page = cursor.fetch(n=5)
    # Then
    assert len(page) == 5
    assert all(not isinstance(p, model.Post) for p in page)


def test_cursor_fetchrow(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    posts.bulk_create(batch, connection=session)
    posts.all(connection=session)
    # When
    with posts.all_cursor(connection=session) as cursor:
        post = cursor.fetchrow()
    # Then
    assert isinstance(post, model.Post)


def test_cursor_fetchrow_raw(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    posts.bulk_create(batch, connection=session)
    # When
    with posts.all_cursor(connection=session, coerce=False) as cursor:
        post = cursor.fetchrow()
    # Then
    assert post and not isinstance(post, model.Post)


def test_cursor_aiter(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    posts.bulk_create(batch, connection=session)
    created = posts.all(connection=session)
    # When
    with posts.all_cursor(connection=session) as cursor:
        fetched = [p for p in cursor]
    assert fetched == created


def test_cursor_forward(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    posts.bulk_create(batch, connection=session)
    created = posts.all(connection=session)
    # When
    with posts.all_cursor(connection=session) as cursor:
        cursor.forward(n=len(created))
        post = cursor.fetchrow()
    # Then
    assert not post


def test_default(posts, post, session):
    # Given
    post = posts.create(model=post, connection=session)
    # When
    fetched = posts.get(id=post.id, connection=session)
    # Then
    assert fetched == post


def test_default_raw(posts, post, session):
    # Given
    post = posts.create(model=post, connection=session, coerce=False)
    # When
    fetched = posts.get(id=post.id, connection=session, coerce=False)
    # Then
    assert fetched == post
