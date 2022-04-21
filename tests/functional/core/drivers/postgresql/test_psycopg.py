import psycopg
import pytest

from yesql.core.drivers.postgresql import _psycopg

pytestmark = pytest.mark.asyncio


class TestConnectionTransactionManagement:
    @staticmethod
    async def test_async_connection(
        async_psycopg_executor: _psycopg.AsyncPsycoPGQueryExecutor,
    ):
        # When
        # We get a connection from the pool
        async with async_psycopg_executor.connection() as c:
            # Pass it through (would stall if we tried to fetch again,
            #   since the pool has only one connection).
            c2: psycopg.AsyncConnection
            async with async_psycopg_executor.connection(connection=c) as c2:
                cursor: psycopg.AsyncCursor = await c2.execute("SELECT 1")
                async with cursor:
                    one = (await cursor.fetchone())[0]
        # Then
        assert one == 1

    @staticmethod
    async def test_async_transaction(
        async_psycopg_executor: _psycopg.AsyncPsycoPGQueryExecutor,
    ):
        # Given
        bar = "Something important."
        # When
        # Start a transaction which we'll roll back
        async with async_psycopg_executor.transaction(rollback=True) as c:
            # Start a nested transaction which will be committed
            async with async_psycopg_executor.transaction(connection=c) as c2:
                cursor = await c2.execute(
                    "INSERT INTO foo (bar) VALUES (%(bar)s) RETURNING *",
                    params={"bar": bar},
                )
                async with cursor:
                    created = await cursor.fetchone()
            # Nested transaction committed
            cursor = await c.execute(
                "SELECT * FROM foo WHERE id=%(id)s", params={"id": created.id}
            )
            async with cursor:
                committed = await cursor.fetchone()
        # Root transaction rolled back
        cursor = await c.execute(
            "SELECT * FROM foo WHERE id=%(id)s", params={"id": created.id}
        )
        async with cursor:
            rolledback = await cursor.fetchone()
        # Then
        assert created == committed and created.bar == bar and rolledback is None

    @staticmethod
    def test_sync_connection(sync_psycopg_executor: _psycopg.PsycoPGQueryExecutor):
        # When
        # We get a connection from the pool
        with sync_psycopg_executor.connection() as c:
            # Pass it through (would stall if we tried to fetch again,
            #   since the pool has only one connection).
            c2: psycopg.Connection
            with sync_psycopg_executor.connection(connection=c) as c2:
                cursor: psycopg.Cursor
                with c2.execute("SELECT 1") as cursor:
                    one = cursor.fetchone()[0]
        # Then
        assert one == 1

    @staticmethod
    def test_sync_transaction(sync_psycopg_executor: _psycopg.PsycoPGQueryExecutor):
        # Given
        bar = "Something important."
        create_sql = "INSERT INTO foo (bar) VALUES (%(bar)s) RETURNING *"
        fetch_sql = "SELECT * FROM foo WHERE id=%(id)s"
        # When
        # Start a transaction which we'll roll back
        with sync_psycopg_executor.transaction(rollback=True) as c:
            # Start a nested transaction which will be committed
            with sync_psycopg_executor.transaction(connection=c) as c2:
                with c2.execute(create_sql, params={"bar": bar}) as cursor:
                    created = cursor.fetchone()
            fetch_params = {"id": created.id}
            # Nested transaction committed
            with c.execute(fetch_sql, params=fetch_params) as cursor:
                committed = cursor.fetchone()
        # Root transaction rolled back
        with c.execute(fetch_sql, params=fetch_params) as cursor:
            rolledback = cursor.fetchone()
        # Then
        assert created == committed and created.bar == bar and rolledback is None
