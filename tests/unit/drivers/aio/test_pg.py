import asyncio
from unittest import mock

import asyncpg
import pytest

from norma.drivers.aio import postgres

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module", autouse=True)
def MockAsyncPGConnection():
    with mock.patch("asyncpg.Connection", autospec=True) as mconn:
        with mock.patch("asyncpg.connect", autospec=True) as mmconn:
            mmconn.return_value = mconn.return_value
            yield mconn


@pytest.fixture(scope="module", autouse=True)
def MockAsyncPGTransaction(MockAsyncPGConnection):
    with mock.patch(
        "asyncpg.connection.transaction.Transaction", autospec=True
    ) as mtran:
        MockAsyncPGConnection.return_value.transaction.return_value = mtran.return_value
        yield mtran


@pytest.fixture(scope="module", autouse=True)
def MockAsyncPGPool(MockAsyncPGConnection):
    conn = MockAsyncPGConnection.return_value
    with mock.patch("asyncpg.Pool", autospec=True) as mpool:
        inst = mpool.return_value
        inst.__aenter__ = mock.AsyncMock(return_value=inst)
        inst.acquire.return_value.__aenter__.return_value = conn
        with mock.patch("asyncpg.create_pool", autospec=True) as mmpool:
            mmpool.return_value = inst
            yield mpool


@pytest.fixture(autouse=True)
def connection(MockAsyncPGConnection, MockAsyncPGPool, MockAsyncPGTransaction):
    conn = MockAsyncPGConnection.return_value
    yield conn
    MockAsyncPGConnection.reset_mock()
    MockAsyncPGPool.reset_mock()


@pytest.fixture(autouse=True)
def transaction(MockAsyncPGTransaction):
    tran = MockAsyncPGTransaction.return_value
    yield tran
    MockAsyncPGTransaction.reset_mock()


@pytest.fixture(scope="module")
def connector() -> postgres.AsyncPGConnector:
    connector = postgres.AsyncPGConnector()
    return connector


class TestAsyncPGConnector:
    @staticmethod
    async def test_initialize(connector: postgres.AsyncPGConnector):
        # Given
        connector.initialized = False
        # When
        await connector.initialize()
        # Then
        assert connector.initialized
        assert connector.pool.__aenter__.called

    @staticmethod
    async def test_initialize_done(connector: postgres.AsyncPGConnector):
        # Given
        connector.initialized = True
        # When
        await connector.initialize()
        # Then
        assert not connector.pool.__aenter__.called

    @staticmethod
    async def test_initialize_concurrent(connector: postgres.AsyncPGConnector):
        # Given
        connector.initialized = False
        # When
        await asyncio.gather(
            connector.initialize(),
            connector.initialize(),
            connector.initialize(),
            connector.initialize(),
            connector.initialize(),
        )
        # Then
        assert connector.pool.__aenter__.call_count == 1

    @staticmethod
    async def test_connection(connector: postgres.AsyncPGConnector):
        # When
        async with connector.connection():
            ...

        # Then
        assert connector.pool.acquire.called

    @staticmethod
    async def test_connection_provided(
        connector: postgres.AsyncPGConnector, connection: asyncpg.Connection
    ):
        # When
        async with connector.connection(connection=connection):
            ...

        # Then
        assert not connector.pool.acquire.called

    @staticmethod
    async def test_transaction(
        connector: postgres.AsyncPGConnector, connection, MockAsyncPGTransaction
    ):
        # Given
        expected = mock.call(connection, None, False, False)
        # When
        async with connector.transaction():
            ...
        # Then
        assert MockAsyncPGTransaction.call_args == expected

    @staticmethod
    async def test_transaction_rollback(
        connector: postgres.AsyncPGConnector, transaction
    ):
        # Given
        expected = mock.call(postgres._Rollback, mock.ANY, mock.ANY)
        # When
        async with connector.transaction(rollback=True):
            ...
        # Then
        assert transaction.__aexit__.call_args == expected

    @staticmethod
    async def test_close(connector: postgres.AsyncPGConnector):
        # When
        await connector.close()
        # Then
        assert connector.pool.close.called

    @staticmethod
    async def test_close_timeout(connector: postgres.AsyncPGConnector):
        # Given
        timeout = 0.001

        async def timesout(*args, **kwargs):
            await asyncio.sleep(timeout + 1)

        connector.pool.close.side_effect = timesout
        # When
        await connector.close(timeout=timeout)
        # Then
        assert connector.pool.terminate.called

    @staticmethod
    def test_open(connector: postgres.AsyncPGConnector):
        # Given
        connector.pool._closed = False
        # Then
        assert connector.open

    @staticmethod
    def test_get_explain_command(connector: postgres.AsyncPGConnector):
        # Given
        expected = connector.EXPLAIN_PREFIX
        # When
        cmd = connector.get_explain_command()
        # Then
        assert cmd == expected

    @staticmethod
    def test_get_explain_command_analyze(connector: postgres.AsyncPGConnector):
        # Given
        expected = f"{connector.EXPLAIN_PREFIX} (ANALYZE, )"
        # When
        cmd = connector.get_explain_command(analyze=True)
        # Then
        assert cmd == expected

    @staticmethod
    def test_get_explain_command_format(connector: postgres.AsyncPGConnector):
        # Given
        expected = f"{connector.EXPLAIN_PREFIX} (ANALYZE, FORMAT json)"
        # When
        cmd = connector.get_explain_command(analyze=True, format="json")
        # Then
        assert cmd == expected
