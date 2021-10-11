import dataclasses
import datetime
from typing import Optional

import inflection
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
    publication_date: Optional[datetime.date] = None
    created_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None

    def __setattr__(self, key, value):
        if key == "title":
            object.__setattr__(self, "slug", inflection.parameterize(value))
        object.__setattr__(self, key, value)
