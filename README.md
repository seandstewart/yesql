# NORMA

NORMA is the No ORM Approach to managing your data. Write your SQL, define your
in-memory data structures, and go.

## Quickstart

TODO

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


## Why NORMA?

NORMA takes a SQL-first approach to data managment:

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

## v1.0.0 Roadmap

- [x] Query Library Bootstrapping
- [x] Dynamic Query Library
- [ ] CLI for stamping new libraries or services
- [ ] Full Documentation Coverage
- [ ] Full Test Coverage
- [ ] Dialect Support
  - [x] Async PostgreSQL (via asyncpg)
  - [x] Async SQLite (via aiosqlite)
  - [ ] Async MySQL
  - [x] Sync PostgreSQL
  - [x] Sync SQLite
  - [ ] Sync MySQL

## License

[MIT](https://sean-dstewart.mit-license.org/)
