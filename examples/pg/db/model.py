import dataclasses
import datetime
from typing import Optional, Set

import typic


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
