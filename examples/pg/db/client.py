import pathlib

import norma

from .model import Post

QUERIES = pathlib.Path(__file__).resolve().parent / "queries"


class PostsMetadata(norma.QueryMetadata):
    __querylib__ = QUERIES
    __tablename__ = "posts"
    __exclude_fields__ = frozenset(("slug",))


class Posts(norma.QueryService[Post]):
    metadata = PostsMetadata
    model = Post
