from __future__ import annotations

import dataclasses
import inspect

import pytest
import yesql
from yesql.core.drivers import base, SupportedDriversT


class TestSyncExecutor:
    @staticmethod
    @pytest.fixture
    def session(
        sync_executor: base.BaseQueryExecutor,
    ) -> tuple[base.BaseQueryExecutor, yesql.types.ConnectionT]:
        with sync_executor.transaction(rollback=True) as c:
            yield sync_executor, c

    @staticmethod
    def test_one(session):
        # Given
        executor, connection = session
        bar = "blah"
        create_qd = _test_query_datum(executor.__driver__)
        select_qd = dataclasses.replace(
            create_qd, sql=f"SELECT * FROM foo WHERE bar={bar!r}"
        )
        # When
        created = executor.one(query=create_qd, connection=connection, bar=bar)
        fetched = executor.one(query=select_qd, connection=connection)
        # Then
        assert created and created == fetched

    @staticmethod
    def test_many(session):
        # Given
        executor, connection = session
        bar = "blah"
        create_qd = _test_query_datum(executor.__driver__)
        select_qd = dataclasses.replace(
            create_qd, sql=f"SELECT * FROM foo WHERE bar={bar!r}"
        )
        # When
        created = executor.many(query=create_qd, connection=connection, bar=bar)
        fetched = executor.many(query=select_qd, connection=connection)
        # Then
        assert created and created == fetched

    @staticmethod
    def test_raw(session):
        # Given
        executor, connection = session
        bar = "blah"
        create_qd = _test_query_datum(executor.__driver__)
        select_qd = dataclasses.replace(
            create_qd, sql=f"SELECT * FROM foo WHERE bar={bar!r}"
        )
        # When
        created = executor.raw(query=create_qd, connection=connection, bar=bar)
        fetched = executor.raw(query=select_qd, connection=connection)
        # Then
        assert created and created == fetched

    @staticmethod
    def test_scalar(session):
        # Given
        executor, connection = session
        bar = "blah"
        create_qd = _test_query_datum(executor.__driver__, returning="bar")
        select_qd = dataclasses.replace(
            create_qd, sql=f"SELECT bar FROM foo WHERE bar={bar!r}"
        )
        # When
        created = executor.scalar(query=create_qd, connection=connection, bar=bar)
        fetched = executor.scalar(query=select_qd, connection=connection)
        # Then
        assert created and created == fetched
        # Then
        assert created and created == fetched == bar

    @staticmethod
    def test_multi(session):
        # Given
        executor, connection = session
        bar = "blah"
        create_qd = _test_query_datum(executor.__driver__)
        select_qd = dataclasses.replace(
            create_qd, sql=f"SELECT count(*) FROM foo WHERE bar={bar!r}"
        )
        # When
        created = executor.multi(
            query=create_qd,
            connection=connection,
            params=[dict(bar=bar)],
            returns=False,
        )
        fetched = executor.scalar(query=select_qd, connection=connection)
        # Then
        assert created and created == fetched == 1

    @staticmethod
    def test_affected(session):
        # Given
        executor, connection = session
        bar = "blah"
        create_qd = _test_query_datum(executor.__driver__)
        select_qd = dataclasses.replace(
            create_qd, sql=f"SELECT count(*) FROM foo WHERE bar={bar!r}"
        )
        # When
        created = executor.affected(query=create_qd, connection=connection, bar=bar)
        fetched = executor.scalar(query=select_qd, connection=connection)
        # Then
        assert created and created == fetched


@pytest.mark.asyncio
class TestAsyncExecutor:
    @staticmethod
    @pytest.fixture
    async def session(
        async_executor: base.BaseQueryExecutor,
    ) -> tuple[base.BaseQueryExecutor, yesql.types.ConnectionT]:
        async with async_executor.transaction(rollback=True) as c:
            yield async_executor, c

    @staticmethod
    async def test_one(session):
        # Given
        executor, connection = session
        bar = "blah"
        create_qd = _test_query_datum(executor.__driver__)
        select_qd = dataclasses.replace(
            create_qd, sql=f"SELECT * FROM foo WHERE bar={bar!r}"
        )
        # When
        created = await executor.one(query=create_qd, connection=connection, bar=bar)
        fetched = await executor.one(query=select_qd, connection=connection)
        # Then
        assert created and created == fetched

    @staticmethod
    async def test_many(session):
        # Given
        executor, connection = session
        bar = "blah"
        create_qd = _test_query_datum(executor.__driver__)
        select_qd = dataclasses.replace(
            create_qd, sql=f"SELECT * FROM foo WHERE bar={bar!r}"
        )
        # When
        created = await executor.many(query=create_qd, connection=connection, bar=bar)
        fetched = await executor.many(query=select_qd, connection=connection)
        # Then
        assert created and created == fetched

    @staticmethod
    async def test_raw(session):
        # Given
        executor, connection = session
        bar = "blah"
        create_qd = _test_query_datum(executor.__driver__)
        select_qd = dataclasses.replace(
            create_qd, sql=f"SELECT * FROM foo WHERE bar={bar!r}"
        )
        # When
        created = await executor.raw(query=create_qd, connection=connection, bar=bar)
        fetched = await executor.raw(query=select_qd, connection=connection)
        # Then
        assert created and created == fetched

    @staticmethod
    async def test_scalar(session):
        # Given
        executor, connection = session
        bar = "blah"
        create_qd = _test_query_datum(executor.__driver__, returning="bar")
        select_qd = dataclasses.replace(
            create_qd, sql=f"SELECT bar FROM foo WHERE bar={bar!r}"
        )
        # When
        created = await executor.scalar(query=create_qd, connection=connection, bar=bar)
        fetched = await executor.scalar(query=select_qd, connection=connection)
        # Then
        assert created and created == fetched
        # Then
        assert created and created == fetched == bar

    @staticmethod
    async def test_multi(session):
        # Given
        executor, connection = session
        bar = "blah"
        create_qd = _test_query_datum(executor.__driver__)
        select_qd = dataclasses.replace(
            create_qd, sql=f"SELECT count(*) FROM foo WHERE bar={bar!r}"
        )
        # When
        await executor.multi(
            query=create_qd,
            connection=connection,
            params=[dict(bar=bar)],
            returns=False,
        )
        fetched = await executor.scalar(query=select_qd, connection=connection)
        # Then
        assert fetched == 1

    @staticmethod
    async def test_affected(session):
        # Given
        executor, connection = session
        bar = "blah"
        create_qd = _test_query_datum(executor.__driver__)
        select_qd = dataclasses.replace(
            create_qd, sql=f"SELECT count(*) FROM foo WHERE bar={bar!r}"
        )
        # When
        created = await executor.affected(
            query=create_qd, connection=connection, bar=bar
        )
        fetched = await executor.scalar(query=select_qd, connection=connection)
        # Then
        assert created and created == fetched


def _test_query_datum(
    driver: SupportedDriversT, *, returning: str = "*"
) -> yesql.parse.QueryDatum:
    sql = DRIVER_TO_DML[driver]
    if returning:
        sql += f" RETURNING {returning}"
    return yesql.parse.QueryDatum(
        name="foo",
        doc="",
        sql=sql,
        signature=inspect.Signature(
            [inspect.Parameter("bar", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        ),
        modifier="one",
        remapping={"bar": 1},
    )


DRIVER_TO_DML = {
    "asyncpg": "INSERT INTO foo (bar) VALUES ($1)",
    "psycopg": "INSERT INTO foo (bar) VALUES (%(bar)s)",
}
