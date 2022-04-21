from __future__ import annotations

import datetime

import psycopg
import pytest

from examples.pg.db import client, model
from tests.integration.repository.postgresql import factories


@pytest.fixture(scope="module")
def posts() -> client.SyncPosts:
    with client.SyncPosts() as posts:
        yield posts


@pytest.fixture
def session(posts) -> psycopg.Connection:
    with posts.executor.transaction(rollback=True) as c:
        yield c


def test_persist(posts, post, session):
    # When
    created: model.Post = posts.create(instance=post, connection=session)
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
        instance=post,
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
    created: model.Post = posts.create(instance=post, connection=session)
    # When
    published = posts.publish(id=created.id, publication_date=None, connection=session)
    # Then
    assert isinstance(published.publication_date, datetime.date)


def test_persist_scalar(posts, post, session):
    # Given
    created: model.Post = posts.create(instance=post, connection=session)
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
    posts.bulk_create(instances=batch, connection=session)
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
    posts.bulk_create(params=(posts.get_kvs(p) for p in batch), connection=session)
    created = posts.all(connection=session)
    # Then
    assert len(created) == len(batch)
    assert {
        (post.title, post.subtitle, post.tagline, post.body) for post in created
    } == expected


def test_cursor(posts, session):
    # Given
    batch = factories.PostFactory.create_batch(size=10)
    posts.bulk_create(instances=batch, connection=session)
    # When
    with posts.all_cursor(connection=session) as cursor:
        page = cursor.fetchmany(size=5)
    # Then
    assert len(page) == 5


def test_default(posts, post, session):
    # Given
    post = posts.create(instance=post, connection=session)
    # When
    fetched = posts.get(id=post.id, connection=session)
    # Then
    assert fetched == post


def test_default_raw(posts, post, session):
    # Given
    post = posts.create(instance=post, connection=session, coerce=False)
    # When
    fetched = posts.get(id=post.id, connection=session, coerce=False)
    # Then
    assert fetched == post


def test_default_mutate_scalar(posts, post, session):
    # Given
    post = posts.create(instance=post, connection=session)
    new_tags = {"click", "here"}
    # When
    all_tags = posts.add_tags(id=post.id, tags=new_tags, connection=session)
    # Then
    assert set(all_tags) & new_tags == new_tags
