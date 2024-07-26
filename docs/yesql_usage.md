# Repository Configuration
To parse raw SQL and return data, yesql needs to know how to talk to your database of choice. The details of parsing and querying are determined by the `dialect`, indicating the expected type of database, and the `driver`, indicating the adapter available to connect to that database. Additionally, you may use the `isaio` configuration parameter to indicate if you'd like async or synchronous behavior, though not all drivers support both. yesql will throw an error if no valid dialect, driver, and isaio configuration combination is available. 

## Dialects
yesql currently supports:
* `postgres`

## Drivers
yesql currently supports:
* [asyncpg](https://magicstack.github.io/asyncpg/current/)
* [pyscopg](https://www.psycopg.org/)

Currently, yesql will default to pyscopg if psycopg is installed. If psycogp is not available, and `isaio` is True, yesql will try to use asyncpg. 

## Database Connection

When constructing a repository, you may set or override the connection settings by passing in relevant keyword parameters. If you do not pass in any connection keyword parameters, yesql will check the environmental variables for configuration values. Connection settings may also be set when initializing the repository executor, if you choose to override the default executor. See the tables below for environmental variable names and default values.

| Environmental Value | Type | Description
|---|---|---
|POSTGRES_CONNECTION_DSN|String|Aliased to `DATABASE_URL` or `POSTGRES_CONNECTION_CONNINFO`. 
|POSTGRES_CONNECTION_DBNAME|String|...
|POSTGRES_CONNECTION_HOST|String|...
|POSTGRES_CONNECTION_PORT|String|...
|POSTGRES_CONNECTION_USER|String|...
|POSTGRES_CONNECTION_PASSWORD|String|...
|POSTGRES_CONNECTION_PASSFILE|String|...
|POSTGRES_CONNECTION_AUTOCOMMIT|Boolean|Defaults to False.
|---|---|---
|POSTGRES_POOL_DSN|String|Aliased to `DATABASE_URL` or `POSTGRES_POOL_CONNINFO`
|POSTGRES_POOL_MIN_SIZE|Int|Defaults to 0.
|POSTGRES_POOL_MAX_SIZE|Int|Defaults to 10.
|POSTGRES_POOL_NAME|String|...
|POSTGRES_POOL_TIMEOUT|Int|Defaults to 0.
|POSTGRES_POOL_MAX_LIFETIME|Float|Defaults to 3600.0
|POSTGRES_POOL_MAX_IDLE|Float|Defaults to 600.0
|POSTGRES_POOL_RECONNECT_TIMEOUT|Float|Defaults to 300.0
|POSTGRES_POOL_NUM_WORKERS|Int|Defaults to 3

## Repository Behavior
To implement a repository:

0. Create your new repository class by extending the AsyncQueryRepository or SyncQueryRepository yesql classes.
0. Provide the model to be returned by the repository as the type when extending the base repository class.
0. Configure your repository class with the raw sql queries that should be parsed and associated with your repository. Queries can be provided as a file path or as a string.
0. Optionally, configure your repository class with a specific table name and any fields that should be excluded from serialization and deserialization. If you do not provide a table name, yesql will default to the snake_case version of your model name.
0. Optionally, configure the table fields to be excluded when deserializing a model. yesql defaults to excluding the following fields: `id`, `created_at`, `updated_at`.
0. Run the `yesql` poetry script to generate the [stub file (*.pyi)](https://peps.python.org/pep-0561/#stub-only-packages) for your repository.
0. Finally, write your query functions to add functionality to the repository's new stubs!

```python
QUERIES = pathlib.Path(__file__).resolve().parent / "queries"

...

# The repository class definition here extends AsyncQueryRepository (step 1)
# It also specifies 'Post' as the repository model type -- AsyncQueryRepository[Post] (step 2)
# The specified model will be used by the serialization/deserialization code.
class AsyncPosts(yesql.AsyncQueryRepository[Post]):
    """An asyncio-native service for querying blog posts."""

    class metadata(yesql.QueryMetadata):
        # The __querylib__ attribute points to a directory of raw sql query files, defined in the constant above. (step 3)
        __querylib__ = QUERIES
        # the __tablename__ attribute specifies the table name for the repository. (step 4)
        # In this case, even if the __tablename__ was not provided, yesql would default to `posts` as a table name. 
        __tablename__ = "posts"
        # Here we override the default __exclude_fields__ value to only exclude the `slug` field. (step 5)
        __exclude_fields__ = frozenset(("slug",))
```

# SQL Configuration
## Paths, Files, and Other Query Sources 
Repositories may be configured with queries via file paths:
```python
__querylib__ = pathlib.Path(__file__).resolve().parent / "queries"
```
or with strings:
```python
__querylib__ = """
-- :name get :one
-- Get a blog post by id.
SELECT * FROM blog.posts WHERE id = :id;
"""
```
String configuration provides the flexibility of loading a string from any source to configure your repository. If you do not specify a `__querylib__` configuration, yesql will default to trying to load `.sql` files from the current working directory. yesql will only try to load `.sql` files from any given directory-- if you want to provide string input from a file, you will need to parse the file in your code before passing it to the repository.

## Modifiers and Preamble Parsing
yesql uses the [sqlparse](https://sqlparse.readthedocs.io/en/latest/) library to read queries. It expects a preamble comment and sql expression for each query, and the preamble should include a name marker (`:name`), the query name and a fetch modifier description or `funcop` such as `:one`. The query name is whatever snake_case string comes after `:name` and before the fetch modifier. It is used to name the repository function that will be created from the query. The fetch modifier specifies what deserialization logic and typing should be applied to the query result. If no valid fetch modifier is provided for a query, yesql will default to `:many`.  

* This query is named `get` and is expected to return one row.: `-- :name get :one`
* This query is named `latest_date` and is expected to return a scalar value: `-- :name latest_date :scalar`
* This query is named `get_all` and is expected to return many rows, using the shorthand version of the fetch modifier: `--:name get_all *`

### Fetch Modifiers
| Modifier | Shorthand | Description
|----|----
| :many | * | Return an iterable list of rows
| :one | ^ | Return a single row
| :scalar | $ | Return a single value
| :multi | ! | Return any affected rows
| :affected | # | Returns the number of affected rows as an integer
| :raw | ~ | Behaves as :many, but with a return type of `ANY` instead of `Iterable`. 

## Query Parsing
After the preamble comment and any other docs, comes the query. yesql is comfortable both named and unnamed query parameters: 
 * Named parameters, such as `:param` or `$param` (psycopg format) or `%(param)s` (asyncpg format)
 * Unnamed parameters, such as `:1` or `$1` (psycopg format) or `%()s` (asyncpg format)

Each repository query function will take parameters as args or kwargs based on this parsing. The format will be normalized into something appropriate for the configured driver.

As an example, for a query such as: 
```sql
--:name get_by_slug :one
SELECT * FROM blog.posts 
WHERE slug = :slug;
```

yesql will generate a repository function something like this:
```python
def get_by_slug(self, id) -> typing.Awaitable[Post]:
  ...
```
See the section on stubgen for more details about yesql function generation.

## Serialization and Deserialization
The default serializer for yesql is the base repository `get_kvs` function, which uses [typical](https://python-typical.org/) to iterate through a given model's fields and return it as a dictionary. The default deserializer for yesql also uses typical's [transmute interface](https://python-typical.org/usage/api/#typictransmute) to convert any given class, json structure, or python literal into the type assigned to the given QueryRepository. The default bulk deserialization function uses the same code as the deserialization function, but returns an `Iterable` of the expected model. 

The default serializer, deserializer, and bulk deserialization functions may all be overriden on the QueryRepository class via the `serdes` property. When overriding seriaization and deserialization in the QueryRepository class, provide your new functions arranged in yesql's `statement.SerDes` class.

## Stubgen
The last step of getting a QueryRepository configured is stubgen via the `yesql` script. This script directs yesql to go through all configured query files, parse them for the function signatures, and generate the [stub file](https://peps.python.org/pep-0561/#stub-only-packages) to describe all expected repository functions. 

You may want to automate running stub generation whenever you update your queries to keep the relevant repositories in sync.
