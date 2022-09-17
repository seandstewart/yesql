import pathlib
from typing import Iterable, cast

import yesql

from .model import Post

QUERIES = pathlib.Path(__file__).resolve().parent / "queries"


class AsyncPosts(yesql.AsyncQueryRepository[Post]):
    """An asyncio-native service for querying blog posts."""

    model = Post

    class metadata(yesql.QueryMetadata):
        __querylib__ = QUERIES
        __tablename__ = "posts"
        __exclude_fields__ = frozenset(("slug",))

    async def bulk_create_returning(
        self,
        posts: Iterable[Post],
        *,
        connection: yesql.types.ConnectionT = None,
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
        query = cast(yesql.parse.QueryDatum, self.queries.mutate.bulk_create_returning)
        return await self.executor.many(
            query,
            posts=[self.get_kvs(p) for p in posts],
            connection=connection,
            transaction=True,
            coerce=coerce,
            deserializer=self.serdes.bulk_deserializer,
        )


SyncPosts = yesql.servicemaker(
    model=Post,
    querylib=QUERIES,
    tablename="posts",
    isaio=False,
    exclude_fields=frozenset(("slug",)),
)
