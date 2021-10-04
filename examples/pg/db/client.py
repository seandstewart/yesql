import pathlib

import norma

from .model import Post

QUERIES = pathlib.Path(__file__).resolve().parent / "queries"


class AsyncPosts(norma.AsyncQueryService[Post]):
    """An asyncio-native service for querying blog posts."""

    class metadata(norma.QueryMetadata):
        __querylib__ = QUERIES
        __tablename__ = "posts"
        __exclude_fields__ = frozenset(("slug",))


SyncPosts = norma.servicemaker(
    model=Post,
    querylib=QUERIES,
    tablename="posts",
    driver="psycopg",
    exclude_fields=frozenset(("slug",)),
)
