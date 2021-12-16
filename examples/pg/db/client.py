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
        __scalar_queries__ = frozenset(("add_tags", "remove_tags"))

    @norma.support.coerceable(bulk=True)
    @norma.support.retry
    async def bulk_create_returning(
        self,
        posts: Iterable[Post],
        *,
        connection: norma.types.ConnectionT = None,
        coerce: bool = True
    ):
        """An example of overriding the default implementation for a specific use-case.

        In this case, the query is implemented to take an array of posts as a single
        input. This allows a bulk operation to occur with a single, atomic query,
        rather than as a series of executions.

        This query isn't compatible with the default implementation of a mutation,
        but as you can see, it's quite straight-forward to customize your query methods
        if necessary.
        """
        raw = [self.get_kvs(p) for p in posts]
        async with self.connector.transaction(connection=connection) as connection:
            return await self.queries.bulk_create_returning(connection, posts=raw)


SyncPosts = norma.servicemaker(
    model=Post,
    querylib=QUERIES,
    tablename="posts",
    driver="psycopg",
    exclude_fields=frozenset(("slug",)),
    scalar_queries=frozenset(("add_tags",)),
)
