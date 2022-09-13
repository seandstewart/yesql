# YeSQL

Say YES to SQL with YeSQL. YeSQL eliminates boilerplate without the baggage of an 
expensive or clunky ORM. Simply write your SQL and point YeSQL to the directory, and it 
does all the rest.

## Quickstart


### Installation

```shell
pip install -U yesql
```

### Basic Usage

```python
import dataclasses
import datetime
import pathlib
from typing import Optional, Set

import typic
import yesql


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



class AsyncPosts(yesql.AsyncQueryRepository[Post]):
    """An asyncio-native service for querying blog posts."""

    class metadata(yesql.QueryMetadata):
        __querylib__ = QUERIES
        __tablename__ = "posts"
        __exclude_fields__ = frozenset(("slug",))


```

## No ORMs?

1. *ORMs are bad for you.*  
   They are a leaky abstraction that cannot solve the problem they set out to do - which
   is to abstract out the details of working with a database.

2. *ORMs are slow.*  
   ORMs depend upon an extremely high level of abstraction in order to work consistently
   across database clients. They also attempt to bridge the gap of data validation and
   state management. By attempting to hide the details of managing state from the end
   user, they suffer from large computational costs and predict

3. *ORMs are wasteful.*  
   As mentioned above, ORMs use a huge amount of resources to auto-magically determine
   state for anything and everything currently pulled into your application's memory.
   Because this is implicit, this takes a large amount of work to do right. In general,
   your application is already aware of "clean" and "dirty" states. Your application
   should be in charge of managing it.

4. *ORMs encourage bad data modeling.*  
   ORMs vastly simplify recursive data modeling (see: N+1 problem) which encourages lazy
   solutions to simple problems and can result in extreme service degradation without
   reasonable means of mitigation.


## Why YeSQL?

YeSQL takes a SQL-first approach to data management:

1. *Focus on your SQL and your database.*
   - Reduce developer overhead by having one less middleman between you and your data.
   - Easily fine-tune your SQL to take full advantage of the RDBMS you choose.
   - No guesswork about what SQL is actually being executed on your database.

2. *Explicit state management.*
   - No surprise writes or reads, you control when you read and when you write.
   - There is no question whether data is in-memory or in the database.

3. *Plain Ol' Data Objects.*
   - Model your data with a mapping, a namedtuple, a dataclass, or just use the native
     record objects of your preferred library.
   - Loose ser/des based on your model, which can be overridden at any point.
   - No implicit state mapping of data from a table to your model. Your query powers 
     your model.

## v1.0.0 Roadmap

- [x] Query Library Bootstrapping
- [x] Dynamic Query Library
- [ ] CLI for stamping new libraries or services
- [ ] Full Documentation Coverage
- [ ] Full Test Coverage
- [ ] Dialect Support
  - [x] Async PostgreSQL (via asyncpg & psycopg3)
  - [ ] Async SQLite (via aiosqlite)
  - [ ] Async MySQL
  - [x] Sync PostgreSQL
  - [ ] Sync SQLite
  - [ ] Sync MySQL

## License

[MIT](https://sean-dstewart.mit-license.org/)
