import pathlib

import norma
import asyncpg
import psycopg

from .model import Post

QUERIES = pathlib.Path(__file__).resolve().parent / "queries"


class AsyncPosts(norma.service.AsyncQueryService[Post, asyncpg.Connection]):
    """An asyncio-native service for querying blog posts."""

    class metadata(norma.service.QueryMetadata):
        __querylib__ = QUERIES
        __tablename__ = "posts"
        __exclude_fields__ = frozenset(("slug",))


class SyncPosts(norma.service.SyncQueryService[Post, psycopg.Connection]):
    """A sync-io service for querying blog posts."""

    class metadata(norma.service.SyncQueryService.metadata):
        __querylib__ = QUERIES
        __tablename__ = "posts"
        __exclude_fields__ = frozenset(("slug",))
