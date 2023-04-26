# yesql

[![image](https://img.shields.io/pypi/v/yesql.svg)](https://pypi.org/project/yesql/)
[![image](https://img.shields.io/pypi/l/yesql.svg)](https://pypi.org/project/yesql/)
[![image](https://img.shields.io/pypi/pyversions/yesql.svg)](https://pypi.org/project/yesql/)
[![image](https://img.shields.io/github/languages/code-size/seandstewart/yesql.svg?style=flat)](https://github.com/seandstewart/yesql)
[![Test & Lint](https://github.com/seandstewart/yesql/workflows/Test/badge.svg)](https://github.com/seandstewart/yesql/actions)
[![Coverage](https://codecov.io/gh/seandstewart/yesql/branch/main/graph/badge.svg)](https://codecov.io/gh/seandstewart/yesql)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)


Say _yes_ to _SQL_ with **yesql**. 

yesql eliminates boilerplate without the baggage of an expensive or clunky ORM. 
Simply write your SQL, point yesql to the directory, and it does all the rest.

## Quickstart

### Installation

```shell
pip install -U --pre yesql
```
or
```shell
poetry add --allow-prereleases yesql
```

yesql currently supports the following database drivers:

- [asyncpg][1]
- [psycopg][2]

You can select your driver as an extra when installing yesql _(recommended)_:

```shell
pip install -U --pre "yesql[psycopg]"
```
or
```shell
poetry add --allow-prereleases yesql -E asyncpg
```

### Basic Usage

```python
from __future__ import annotations

import dataclasses
import datetime
import pathlib

import yesql


QUERIES = pathlib.Path(__file__).resolve().parent / "queries"


@dataclasses.dataclass(slots=True, kw_only=True)
class Post:
    id: int | None = None
    slug: str | None = None
    title: str | None = None
    subtitle: str | None = None
    tagline: str | None = None
    body: str | None = None
    tags: set[str] = dataclasses.field(default_factory=set)
    publication_date: datetime.date | None = None
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None



class PostsRepository(yesql.SyncQueryRepository[Post]):
    """An asyncio-native service for querying blog posts."""

    class metadata(yesql.QueryMetadata):
        __querylib__ = QUERIES
        __tablename__ = "posts"
        __exclude_fields__ = frozenset(("slug",))



posts = PostsRepository()
posts.initialize()
new_post = Post(
    title="My Great Blog Post",
    subtitle="It's super great. Trust me...",
    tagline="You'll be glad you read it.",
    tags={"tips", "tricks", "cool stuff"},
)
saved_post = posts.create(instance=new_post)
```

#### Type-stub Generation (Experimental)

yesql ships with simple CLI for generating type-stubs. This allows for more exact 
static type-analysis and enables auto-complete for your IDE.

Usage:

```shell
yesql stubgen
```

You can optionally supply any number of paths to directories or python modules. The 
command will default to the current working directory on the filesystem.

If you don't have [black][3] installed in your development environment, you should add 
the `cli` extra as a development dependency.

## Features

- [x] Support for synchronous IO
- [x] Support for asynchronous IO (asyncio)
- [x] Support for PostgreSQL
- [x] Plays well with MyPy
- [x] Plays well with IDEs
- [x] Encourages best-practices for data-access (Separation of Concerns, Repository 
  Pattern)

## No ORMs?

1. *ORMs are bad for you.*  
   They are a leaky abstraction that cannot solve the problem they set out to do - which
   is to abstract out the details of working with a database.

2. *ORMs are slow.*  
   ORMs depend upon a high level of abstraction in order to work across database 
   clients. They also attempt to bridge the gap of data validation and  state 
   management. By attempting to hide the details of managing state from the end
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


## Why yesql?

yesql takes a SQL-first approach to data management:

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
- [ ] Full Documentation Coverage
- [ ] Full Test Coverage
- [ ] Dialect Support
  - [x] Async PostgreSQL (via asyncpg & psycopg3)
  - [ ] Async SQLite
  - [ ] Async MySQL
  - [x] Sync PostgreSQL
  - [ ] Sync SQLite
  - [ ] Sync MySQL

## License

[MIT](https://sean-dstewart.mit-license.org/)


[1]: https://magicstack.github.io/asyncpg/current/
[2]: https://www.psycopg.org/psycopg3/docs/
[3]: https://black.readthedocs.io/en/stable/