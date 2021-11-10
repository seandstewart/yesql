import asyncio
from unittest import mock

import aiosqlite
import pytest

from norma.drivers.aio import sqlite

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module", autouse=True)
def MockAIOSQLiteConnection():
    with mock.patch("aiosqlite.Connection", autospec=True) as mconn:
        inst = mconn.return_value
        inst.__aenter__.return_value = inst
        inst.execute.side_effect = mock.AsyncMock()
        with mock.patch("aiosqlite.connect", autospec=True) as mmconn:
            mmconn.return_value = inst
            yield mconn


@pytest.fixture(autouse=True)
def connection(MockAIOSQLiteConnection):
    conn = MockAIOSQLiteConnection.return_value
    yield conn
    MockAIOSQLiteConnection.reset_mock()
    aiosqlite.connect.reset_mock()


@pytest.fixture(scope="module")
def connector() -> sqlite.AIOSQLiteConnector:
    connector = sqlite.AIOSQLiteConnector(database="foo")
    return connector


class TestAIOSQLiteConnector:
    @staticmethod
    async def test_initialize(connector: sqlite.AIOSQLiteConnector, connection):
        # Given
        connector.initialized = False
        # When
        await connector.initialize()
        # Then
        assert connector.initialized
        assert connection.execute.called

    @staticmethod
    async def test_initialize_done(connector: sqlite.AIOSQLiteConnector, connection):
        # Given
        connector.initialized = True
        # When
        await connector.initialize()
        # Then
        assert not connection.execute.called

    @staticmethod
    async def test_initialize_concurrent(
        connector: sqlite.AIOSQLiteConnector, connection
    ):
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
        assert connection.execute.call_count == 1

    @staticmethod
    async def test_connection(connector: sqlite.AIOSQLiteConnector, connection):
        # When
        async with connector.connection():
            ...

        # Then
        assert aiosqlite.connect.called
        assert connection.rollback.called

    @staticmethod
    async def test_connection_no_transaction(
        connector: sqlite.AIOSQLiteConnector, connection
    ):
        # Given
        connection.in_transaction = False
        # When
        async with connector.connection():
            ...

        # Then
        assert aiosqlite.connect.called
        assert not connection.rollback.called

    @staticmethod
    async def test_connection_provided(
        connector: sqlite.AIOSQLiteConnector, connection: aiosqlite.Connection
    ):
        # When
        async with connector.connection(connection=connection):
            ...

        # Then
        assert not aiosqlite.connect.called
        assert not connection.rollback.called

    @staticmethod
    async def test_transaction(connector: sqlite.AIOSQLiteConnector, connection):
        # When
        async with connector.transaction():
            ...
        # Then
        assert connection.commit.called

    @staticmethod
    async def test_transaction_rollback(
        connector: sqlite.AIOSQLiteConnector, connection
    ):
        # When
        async with connector.transaction(rollback=True):
            ...
        # Then
        assert not connection.commit.called

    @staticmethod
    async def test_close(connector: sqlite.AIOSQLiteConnector):
        # When
        await connector.close()
        # Then
        assert connector.initialized is False

    @staticmethod
    def test_open(connector: sqlite.AIOSQLiteConnector):
        # Given
        connector.initialized = True
        # Then
        assert connector.open

    @staticmethod
    def test_get_explain_command(connector: sqlite.AIOSQLiteConnector):
        # Given
        expected = connector.EXPLAIN_PREFIX
        # When
        cmd = connector.get_explain_command()
        # Then
        assert cmd == expected

    @staticmethod
    def test_get_explain_command_analyze(connector: sqlite.AIOSQLiteConnector):
        # Given
        expected = connector.EXPLAIN_PREFIX
        # When
        cmd = connector.get_explain_command(analyze=True)
        # Then
        assert cmd == expected

    @staticmethod
    def test_get_explain_command_format(connector: sqlite.AIOSQLiteConnector):
        # Given
        expected = connector.EXPLAIN_PREFIX
        # When
        cmd = connector.get_explain_command(analyze=True, format="json")
        # Then
        assert cmd == expected
