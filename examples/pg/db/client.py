import pathlib

import norma
import asyncpg

from .model import Post

QUERIES = pathlib.Path(__file__).resolve().parent / "queries"


class Posts(norma.service.AsyncQueryService[Post, asyncpg.Connection]):
    """A service for querying blog posts."""

    class metadata(norma.service.QueryMetadata):
        __querylib__ = QUERIES
        __tablename__ = "posts"
        __exclude_fields__ = frozenset(("slug",))
