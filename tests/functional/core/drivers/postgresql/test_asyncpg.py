import pytest

from yesql.core.drivers.postgresql import _asyncpg

pytestmark = pytest.mark.asyncio


class TestConnectionTransactionManagement:
    @staticmethod
    async def test_connection(asyncpg_executor: _asyncpg.AsyncPGQueryExecutor):
        # When
        # We get a connection from the pool
        async with asyncpg_executor.connection() as c:
            # Pass it through (would stall if we tried to fetch again,
            #   since the pool has only one connection).
            async with asyncpg_executor.connection(connection=c) as c2:
                one = await c2.fetchval("SELECT 1")

        # Then
        assert one == 1

    @staticmethod
    async def test_transaction(asyncpg_executor: _asyncpg.AsyncPGQueryExecutor):
        # Given
        bar = "Something important."
        # When
        # Start a transaction which we'll roll back
        async with asyncpg_executor.transaction(rollback=True) as c:
            # Start a nested transaction which will be committed
            async with asyncpg_executor.transaction(connection=c) as c2:
                created = await c2.fetchrow(
                    "INSERT INTO foo (bar) VALUES ($1) RETURNING *", bar
                )
            # Nested transaction committed
            committed = await c2.fetchrow(
                "SELECT * FROM foo WHERE id=$1", created["id"]
            )
        # Root transaction rolled back
        rolledback = await asyncpg_executor.pool.fetchrow(
            "SELECT * FROM foo WHERE id=$1", created["id"]
        )
        # Then
        assert created == committed and created["bar"] == bar and rolledback is None
