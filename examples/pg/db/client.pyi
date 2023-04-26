import typing
import pathlib
from typing import Iterable, cast

import yesql

from .model import Post

QUERIES = pathlib.Path(__file__).resolve().parent / "queries"

class AsyncPosts(yesql.AsyncQueryRepository[Post]):
    """An asyncio-native service for querying blog posts."""

    model = Post

    class metadata(yesql.QueryMetadata):
        __querylib__ = QUERIES
        __tablename__ = "posts"
        __exclude_fields__ = frozenset(("slug",))
    async def bulk_create_returning(
        self,
        posts: Iterable[Post],
        *,
        connection: yesql.types.ConnectionT = None,
        coerce: bool = True,
    ):
        """An example of overriding the default implementation for a specific use-case.

        In this case, the query is implemented to take an array of posts as a single
        input. This allows a bulk operation to occur with a single, atomic query,
        rather than as a series of executions.

        This query isn't compatible with the default implementation of a mutation,
        but as you can see, it's quite straight-forward to customize your query methods
        if necessary.
        """
        query = cast(yesql.parse.QueryDatum, self.queries.mutate.bulk_create_returning)
        return await self.executor.many(
            query,
            posts=[self.get_kvs(p) for p in posts],
            connection=connection,
            transaction=True,
            coerce=coerce,
            deserializer=self.serdes.bulk_deserializer,
        )
    @typing.overload
    def get(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """Get a blog post by id."""
    @typing.overload
    def get(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """Get a blog post by id."""
    @typing.overload
    def get(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Get a blog post by id."""
    @typing.overload
    def get(
        self,
        /,
        *,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """Get a blog post by id."""
    @typing.overload
    def get(
        self,
        /,
        *,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """Get a blog post by id."""
    @typing.overload
    def get(
        self,
        /,
        *,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Get a blog post by id."""
    @typing.overload
    def get_by_slug(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """Get a blog post by slug."""
    @typing.overload
    def get_by_slug(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """Get a blog post by slug."""
    @typing.overload
    def get_by_slug(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Get a blog post by slug."""
    @typing.overload
    def get_by_slug(
        self,
        /,
        *,
        slug,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """Get a blog post by slug."""
    @typing.overload
    def get_by_slug(
        self,
        /,
        *,
        slug,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """Get a blog post by slug."""
    @typing.overload
    def get_by_slug(
        self,
        /,
        *,
        slug,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Get a blog post by slug."""
    @typing.overload
    def get_by_tags(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Get blog posts matching the given tags."""
    @typing.overload
    def get_by_tags(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Get blog posts matching the given tags."""
    @typing.overload
    def get_by_tags(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Get blog posts matching the given tags."""
    @typing.overload
    def get_by_tags(
        self,
        /,
        *,
        tags,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Get blog posts matching the given tags."""
    @typing.overload
    def get_by_tags(
        self,
        /,
        *,
        tags,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Get blog posts matching the given tags."""
    @typing.overload
    def get_by_tags(
        self,
        /,
        *,
        tags,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Get blog posts matching the given tags."""
    @typing.overload
    def get_by_tags_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get blog posts matching the given tags."""
    @typing.overload
    def get_by_tags_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get blog posts matching the given tags."""
    @typing.overload
    def get_by_tags_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get blog posts matching the given tags."""
    @typing.overload
    def get_by_tags_cursor(
        self,
        /,
        *,
        tags,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get blog posts matching the given tags."""
    @typing.overload
    def get_by_tags_cursor(
        self,
        /,
        *,
        tags,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get blog posts matching the given tags."""
    @typing.overload
    def get_by_tags_cursor(
        self,
        /,
        *,
        tags,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get blog posts matching the given tags."""
    @typing.overload
    def all(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Get all blog posts."""
    @typing.overload
    def all(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Get all blog posts."""
    @typing.overload
    def all(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Get all blog posts."""
    @typing.overload
    def all(
        self,
        /,
        *,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Get all blog posts."""
    @typing.overload
    def all(
        self,
        /,
        *,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Get all blog posts."""
    @typing.overload
    def all(
        self,
        /,
        *,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Get all blog posts."""
    @typing.overload
    def all_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get all blog posts."""
    @typing.overload
    def all_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get all blog posts."""
    @typing.overload
    def all_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get all blog posts."""
    @typing.overload
    def all_cursor(
        self,
        /,
        *,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get all blog posts."""
    @typing.overload
    def all_cursor(
        self,
        /,
        *,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get all blog posts."""
    @typing.overload
    def all_cursor(
        self,
        /,
        *,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get all blog posts."""
    @typing.overload
    def published(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Get all blog posts which have been published up to this date."""
    @typing.overload
    def published(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Get all blog posts which have been published up to this date."""
    @typing.overload
    def published(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Get all blog posts which have been published up to this date."""
    @typing.overload
    def published(
        self,
        /,
        *,
        date,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Get all blog posts which have been published up to this date."""
    @typing.overload
    def published(
        self,
        /,
        *,
        date,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Get all blog posts which have been published up to this date."""
    @typing.overload
    def published(
        self,
        /,
        *,
        date,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Get all blog posts which have been published up to this date."""
    @typing.overload
    def published_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get all blog posts which have been published up to this date."""
    @typing.overload
    def published_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get all blog posts which have been published up to this date."""
    @typing.overload
    def published_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get all blog posts which have been published up to this date."""
    @typing.overload
    def published_cursor(
        self,
        /,
        *,
        date,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get all blog posts which have been published up to this date."""
    @typing.overload
    def published_cursor(
        self,
        /,
        *,
        date,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get all blog posts which have been published up to this date."""
    @typing.overload
    def published_cursor(
        self,
        /,
        *,
        date,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Get all blog posts which have been published up to this date."""
    @typing.overload
    def search(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Search all blog posts using full-text a generalized word search."""
    @typing.overload
    def search(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Search all blog posts using full-text a generalized word search."""
    @typing.overload
    def search(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Search all blog posts using full-text a generalized word search."""
    @typing.overload
    def search(
        self,
        /,
        *,
        words,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Search all blog posts using full-text a generalized word search."""
    @typing.overload
    def search(
        self,
        /,
        *,
        words,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Search all blog posts using full-text a generalized word search."""
    @typing.overload
    def search(
        self,
        /,
        *,
        words,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Search all blog posts using full-text a generalized word search."""
    @typing.overload
    def search_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Search all blog posts using full-text a generalized word search."""
    @typing.overload
    def search_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Search all blog posts using full-text a generalized word search."""
    @typing.overload
    def search_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Search all blog posts using full-text a generalized word search."""
    @typing.overload
    def search_cursor(
        self,
        /,
        *,
        words,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Search all blog posts using full-text a generalized word search."""
    @typing.overload
    def search_cursor(
        self,
        /,
        *,
        words,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Search all blog posts using full-text a generalized word search."""
    @typing.overload
    def search_cursor(
        self,
        /,
        *,
        words,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Search all blog posts using full-text a generalized word search."""
    @typing.overload
    def search_phrase(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Search all blog posts for a particular phrase."""
    @typing.overload
    def search_phrase(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Search all blog posts for a particular phrase."""
    @typing.overload
    def search_phrase(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Search all blog posts for a particular phrase."""
    @typing.overload
    def search_phrase(
        self,
        /,
        *,
        phrase,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Search all blog posts for a particular phrase."""
    @typing.overload
    def search_phrase(
        self,
        /,
        *,
        phrase,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Search all blog posts for a particular phrase."""
    @typing.overload
    def search_phrase(
        self,
        /,
        *,
        phrase,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Search all blog posts for a particular phrase."""
    @typing.overload
    def search_phrase_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Search all blog posts for a particular phrase."""
    @typing.overload
    def search_phrase_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Search all blog posts for a particular phrase."""
    @typing.overload
    def search_phrase_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Search all blog posts for a particular phrase."""
    @typing.overload
    def search_phrase_cursor(
        self,
        /,
        *,
        phrase,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Search all blog posts for a particular phrase."""
    @typing.overload
    def search_phrase_cursor(
        self,
        /,
        *,
        phrase,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Search all blog posts for a particular phrase."""
    @typing.overload
    def search_phrase_cursor(
        self,
        /,
        *,
        phrase,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Search all blog posts for a particular phrase."""
    @typing.overload
    def create(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """Create a new blog post :)"""
    @typing.overload
    def create(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """Create a new blog post :)"""
    @typing.overload
    def create(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Create a new blog post :)"""
    @typing.overload
    def create(
        self,
        /,
        *,
        title,
        subtitle,
        tagline,
        tags,
        body,
        publication_date,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """Create a new blog post :)"""
    @typing.overload
    def create(
        self,
        /,
        *,
        title,
        subtitle,
        tagline,
        tags,
        body,
        publication_date,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """Create a new blog post :)"""
    @typing.overload
    def create(
        self,
        /,
        *,
        title,
        subtitle,
        tagline,
        tags,
        body,
        publication_date,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create(
        self,
        /,
        *,
        instances: "typing.Sequence[Post]" = (),
        params: "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]" = (),
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        returns: "bool" = False,
        deserializer: "yesql.types.DeserializerT | None" = None,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create(
        self,
        /,
        *,
        instances: "typing.Sequence[Post]" = (),
        params: "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]" = (),
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        returns: "bool" = False,
        deserializer: "yesql.types.DeserializerT | None" = None,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create(
        self,
        /,
        *,
        instances: "typing.Sequence[Post]" = (),
        params: "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]" = (),
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        returns: "bool" = False,
        deserializer: "yesql.types.DeserializerT | None" = None,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create(
        self,
        /,
        *,
        title,
        subtitle,
        tagline,
        tags,
        body,
        publication_date,
        params: "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]" = (),
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        returns: "bool" = False,
        deserializer: "yesql.types.DeserializerT | None" = None,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create(
        self,
        /,
        *,
        title,
        subtitle,
        tagline,
        tags,
        body,
        publication_date,
        params: "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]" = (),
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        returns: "bool" = False,
        deserializer: "yesql.types.DeserializerT | None" = None,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create(
        self,
        /,
        *,
        title,
        subtitle,
        tagline,
        tags,
        body,
        publication_date,
        params: "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]" = (),
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        returns: "bool" = False,
        deserializer: "yesql.types.DeserializerT | None" = None,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_cursor(
        self,
        /,
        *,
        instances: "typing.Sequence[Post]" = (),
        params: "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]" = (),
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_cursor(
        self,
        /,
        *,
        instances: "typing.Sequence[Post]" = (),
        params: "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]" = (),
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_cursor(
        self,
        /,
        *,
        instances: "typing.Sequence[Post]" = (),
        params: "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]" = (),
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_cursor(
        self,
        /,
        *,
        title,
        subtitle,
        tagline,
        tags,
        body,
        publication_date,
        params: "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]" = (),
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_cursor(
        self,
        /,
        *,
        title,
        subtitle,
        tagline,
        tags,
        body,
        publication_date,
        params: "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]" = (),
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_cursor(
        self,
        /,
        *,
        title,
        subtitle,
        tagline,
        tags,
        body,
        publication_date,
        params: "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]" = (),
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_returning(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_returning(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_returning(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_returning(
        self,
        /,
        *,
        posts,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[list[Post]]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_returning(
        self,
        /,
        *,
        posts,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[list[Post]]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_returning(
        self,
        /,
        *,
        posts,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_returning_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_returning_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_returning_cursor(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_returning_cursor(
        self,
        /,
        *,
        posts,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_returning_cursor(
        self,
        /,
        *,
        posts,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Create a new blog post :)"""
    @typing.overload
    def bulk_create_returning_cursor(
        self,
        /,
        *,
        posts,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.AsyncContextManager[yesql.types.CursorT]":
        """Create a new blog post :)"""
    @typing.overload
    def update(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """Update a post with all new data."""
    @typing.overload
    def update(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """Update a post with all new data."""
    @typing.overload
    def update(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Update a post with all new data."""
    @typing.overload
    def update(
        self,
        /,
        *,
        title,
        subtitle,
        tagline,
        tags,
        body,
        publication_date,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """Update a post with all new data."""
    @typing.overload
    def update(
        self,
        /,
        *,
        title,
        subtitle,
        tagline,
        tags,
        body,
        publication_date,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """Update a post with all new data."""
    @typing.overload
    def update(
        self,
        /,
        *,
        title,
        subtitle,
        tagline,
        tags,
        body,
        publication_date,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Update a post with all new data."""
    @typing.overload
    def delete(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """Delete a post."""
    @typing.overload
    def delete(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """Delete a post."""
    @typing.overload
    def delete(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Delete a post."""
    @typing.overload
    def delete(
        self,
        /,
        *,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """Delete a post."""
    @typing.overload
    def delete(
        self,
        /,
        *,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """Delete a post."""
    @typing.overload
    def delete(
        self,
        /,
        *,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Delete a post."""
    @typing.overload
    def publish(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """Set the publication date for a blog post."""
    @typing.overload
    def publish(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """Set the publication date for a blog post."""
    @typing.overload
    def publish(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Set the publication date for a blog post."""
    @typing.overload
    def publish(
        self,
        /,
        *,
        publication_date,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """Set the publication date for a blog post."""
    @typing.overload
    def publish(
        self,
        /,
        *,
        publication_date,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """Set the publication date for a blog post."""
    @typing.overload
    def publish(
        self,
        /,
        *,
        publication_date,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Set the publication date for a blog post."""
    @typing.overload
    def retract(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """ "Retract" a blog post by clearing out the publication_date."""
    @typing.overload
    def retract(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """ "Retract" a blog post by clearing out the publication_date."""
    @typing.overload
    def retract(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """ "Retract" a blog post by clearing out the publication_date."""
    @typing.overload
    def retract(
        self,
        /,
        *,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
    ) -> "typing.Awaitable[Post]":
        """ "Retract" a blog post by clearing out the publication_date."""
    @typing.overload
    def retract(
        self,
        /,
        *,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[Post]":
        """ "Retract" a blog post by clearing out the publication_date."""
    @typing.overload
    def retract(
        self,
        /,
        *,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT" = None,
        deserializer: "yesql.types.DeserializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """ "Retract" a blog post by clearing out the publication_date."""
    @typing.overload
    def add_tags(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.Awaitable[typing.Any]":
        """Add new tags for a blog post."""
    @typing.overload
    def add_tags(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[typing.Any]":
        """Add new tags for a blog post."""
    @typing.overload
    def add_tags(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Add new tags for a blog post."""
    @typing.overload
    def add_tags(
        self,
        /,
        *,
        tags,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.Awaitable[typing.Any]":
        """Add new tags for a blog post."""
    @typing.overload
    def add_tags(
        self,
        /,
        *,
        tags,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[typing.Any]":
        """Add new tags for a blog post."""
    @typing.overload
    def add_tags(
        self,
        /,
        *,
        tags,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Add new tags for a blog post."""
    @typing.overload
    def remove_tag(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.Awaitable[typing.Any]":
        """Remove tags for this blog post"""
    @typing.overload
    def remove_tag(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[typing.Any]":
        """Remove tags for this blog post"""
    @typing.overload
    def remove_tag(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Remove tags for this blog post"""
    @typing.overload
    def remove_tag(
        self,
        /,
        *,
        tags,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.Awaitable[typing.Any]":
        """Remove tags for this blog post"""
    @typing.overload
    def remove_tag(
        self,
        /,
        *,
        tags,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[typing.Any]":
        """Remove tags for this blog post"""
    @typing.overload
    def remove_tag(
        self,
        /,
        *,
        tags,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[typing.Any]":
        """Remove tags for this blog post"""
    @typing.overload
    def clear_tags(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.Awaitable[int]":
        """ """
    @typing.overload
    def clear_tags(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[int]":
        """ """
    @typing.overload
    def clear_tags(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[int]":
        """ """
    @typing.overload
    def clear_tags(
        self,
        /,
        *,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.Awaitable[int]":
        """ """
    @typing.overload
    def clear_tags(
        self,
        /,
        *,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[int]":
        """ """
    @typing.overload
    def clear_tags(
        self,
        /,
        *,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[int]":
        """ """
    @typing.overload
    def set_body(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.Awaitable[int]":
        """ """
    @typing.overload
    def set_body(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[int]":
        """ """
    @typing.overload
    def set_body(
        self,
        /,
        *,
        instance: "Post | None" = None,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[int]":
        """ """
    @typing.overload
    def set_body(
        self,
        /,
        *,
        body,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
    ) -> "typing.Awaitable[int]":
        """ """
    @typing.overload
    def set_body(
        self,
        /,
        *,
        body,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[True]",
    ) -> "typing.Awaitable[int]":
        """ """
    @typing.overload
    def set_body(
        self,
        /,
        *,
        body,
        id,
        connection: "yesql.types.ConnectionT" = None,
        timeout: "float" = 10,
        transaction: "bool" = True,
        rollback: "bool" = False,
        serializer: "yesql.types.SerializerT | None" = None,
        coerce: "typing.Literal[False]",
    ) -> "typing.Awaitable[int]":
        """ """

SyncPosts = yesql.servicemaker(
    model=Post,
    querylib=QUERIES,
    tablename="posts",
    isaio=False,
    exclude_fields=frozenset(("slug",)),
)
