import pathlib
from typing import Iterable

import norma

from .model import Post

QUERIES = pathlib.Path(__file__).resolve().parent / "queries"


class AsyncPosts(norma.AsyncQueryService[Post]):
    """An asyncio-native service for querying blog posts."""

    class metadata(norma.QueryMetadata):
        __querylib__ = QUERIES
        __tablename__ = "posts"
        __exclude_fields__ = frozenset(("slug",))

    @norma.support.coerceable(bulk=True)
    @norma.support.retry
    async def bulk_create_returning(
        self,
        posts: Iterable[Post],
        *,
        connection: norma.types.ConnectionT = None,
        coerce: bool = True
    ):
        raw = [self.get_kvs(p) for p in posts]
        async with self.connector.transaction(connection=connection) as connection:
            return await self.queries.bulk_create_returning(connection, posts=raw)


SyncPosts = norma.servicemaker(
    model=Post,
    querylib=QUERIES,
    tablename="posts",
    driver="psycopg",
    exclude_fields=frozenset(("slug",)),
)
