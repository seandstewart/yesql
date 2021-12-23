import pathlib

import yesql

from .model import Post

QUERIES = pathlib.Path(__file__).resolve().parent / "queries"


class AsyncPosts(yesql.AsyncQueryService[Post]):
    """An asyncio-native service for querying blog posts."""

    class metadata(yesql.QueryMetadata):
        __driver__ = "aiosqlite"  # type: ignore
        __querylib__ = QUERIES
        __tablename__ = "posts"

    @yesql.middleware("get")
    def intercept_gets(
        self,
        query: yesql.types.QueryMethodProtocolT,
        *args,
        connection: yesql.types.ConnectionT = None,
        **kwargs,
    ):
        print(f"Intercepted {query.__name__!r}")
        return query(self, *args, connection=connection, **kwargs)


SyncPosts = yesql.servicemaker(
    model=Post,
    querylib=QUERIES,
    tablename="posts",
    driver="sqlite",
)
