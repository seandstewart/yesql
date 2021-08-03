import pathlib

import typic
import norma
from norma import drivers

from .model import Post

QUERIES = pathlib.Path(__file__).resolve().parent / "queries"


class PostsMetadata(norma.QueryMetadata):
    __querylib__ = QUERIES
    __tablename__ = "posts"
    __exclude_fields__ = frozenset(("slug",))


class Posts(norma.QueryService):
    metadata = PostsMetadata
    model = Post

    def __init__(
        self,
        dsn: str = None,
        /,
        *,
        connector: drivers.AsyncPGConnector = None,
        **connect_kwargs,
    ):
        dsn = dsn or typic.environ.str("DATABASE_URL")
        connector = connector or drivers.AsyncPGConnector(dsn, **connect_kwargs)
        super().__init__(connector=connector, **connect_kwargs)
