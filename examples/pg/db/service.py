import dataclasses
import datetime
import pathlib
from typing import Optional, Set

import typic
from norma import client
from norma.drivers import asyncpg


QUERIES = pathlib.Path(__file__).resolve().parent / "queries"


@typic.slotted(dict=False, weakref=True)
@dataclasses.dataclass
class Post:
    id: Optional[int] = None
    slug: Optional[str] = None
    title: Optional[str] = None
    subtitle: Optional[str] = None
    tagline: Optional[str] = None
    body: Optional[str] = None
    tags: Set[str] = dataclasses.field(default_factory=set)
    publication_date: Optional[datetime.date] = None
    created_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None


class Metadata(client.Metadata):
    __querylib__ = QUERIES
    __tablename__ = "posts"
    __exclude_fields__ = frozenset(("slug",))


class Posts(client.QueryService):
    metadata = Metadata
    model = Post

    def __init__(
        self,
        dsn: str = None,
        /,
        *,
        connector: asyncpg.AsyncPGConnector = None,
        **connect_kwargs,
    ):
        dsn = dsn or typic.environ.str("DATABASE_URL")
        connector = connector or asyncpg.AsyncPGConnector(dsn, **connect_kwargs)
        super().__init__(connector=connector, **connect_kwargs)
