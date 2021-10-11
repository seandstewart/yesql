import pathlib

import norma

from .model import Post

QUERIES = pathlib.Path(__file__).resolve().parent / "queries"


class AsyncPosts(norma.AsyncQueryService[Post]):
    """An asyncio-native service for querying blog posts."""

    class metadata(norma.QueryMetadata):
        __driver__ = "aiosqlite"  # type: ignore
        __querylib__ = QUERIES
        __tablename__ = "posts"

    @norma.middleware("get")
    def intercept_gets(
        self,
        query: norma.types.QueryMethodProtocolT,
        *args,
        connection: norma.types.ConnectionT = None,
        **kwargs,
    ):
        print(f"Intercepted {query.__name__!r}")
        return query(self, *args, connection=connection, **kwargs)


SyncPosts = norma.servicemaker(
    model=Post,
    querylib=QUERIES,
    tablename="posts",
    driver="sqlite",
)
