import concurrent.futures

import sqlite3
import pytest

from yesql.drivers.sio import sqlite

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def connection(MockSQLiteConnection):
    conn = MockSQLiteConnection.return_value
    yield conn
    MockSQLiteConnection.reset_mock()
    sqlite3.connect.reset_mock()


@pytest.fixture(scope="module")
def connector() -> sqlite.SQLiteConnector:
    connector = sqlite.SQLiteConnector(database="foo")
    return connector


class TestSQLiteConnector:
    @staticmethod
    def test_initialize(connector: sqlite.SQLiteConnector, connection):
        # Given
        connector.initialized = False
        # When
        connector.initialize()
        # Then
        assert connector.initialized
        assert connection.execute.called

    @staticmethod
    def test_initialize_done(connector: sqlite.SQLiteConnector, connection):
        # Given
        connector.initialized = True
        # When
        connector.initialize()
        # Then
        assert not connection.execute.called

    @staticmethod
    def test_initialize_concurrent(connector: sqlite.SQLiteConnector, connection):
        # Given
        connector.initialized = False
        # When
        with concurrent.futures.ThreadPoolExecutor() as pool:
            futs = (
                pool.submit(connector.initialize),
                pool.submit(connector.initialize),
                pool.submit(connector.initialize),
                pool.submit(connector.initialize),
                pool.submit(connector.initialize),
            )

        concurrent.futures.wait(futs)
        # Then
        assert connection.execute.call_count == 1

    @staticmethod
    def test_connection(connector: sqlite.SQLiteConnector, connection):
        # When
        with connector.connection():
            ...

        # Then
        assert sqlite3.connect.called
        assert connection.rollback.called

    @staticmethod
    def test_connection_no_transaction(connector: sqlite.SQLiteConnector, connection):
        # Given
        connection.in_transaction = False
        # When
        with connector.connection():
            ...

        # Then
        assert sqlite3.connect.called
        assert not connection.rollback.called

    @staticmethod
    def test_connection_provided(
        connector: sqlite.SQLiteConnector, connection: sqlite3.Connection
    ):
        # When
        with connector.connection(connection=connection):
            ...

        # Then
        assert not sqlite3.connect.called
        assert not connection.rollback.called

    @staticmethod
    def test_transaction(connector: sqlite.SQLiteConnector, connection):
        # When
        with connector.transaction():
            ...
        # Then
        assert connection.commit.called

    @staticmethod
    def test_transaction_rollback(connector: sqlite.SQLiteConnector, connection):
        # When
        with connector.transaction(rollback=True):
            ...
        # Then
        assert not connection.commit.called

    @staticmethod
    def test_close(connector: sqlite.SQLiteConnector):
        # When
        connector.close()
        # Then
        assert connector.initialized is False

    @staticmethod
    def test_open(connector: sqlite.SQLiteConnector):
        # Given
        connector.initialized = True
        # Then
        assert connector.open

    @staticmethod
    def test_get_explain_command(connector: sqlite.SQLiteConnector):
        # Given
        expected = connector.EXPLAIN_PREFIX
        # When
        cmd = connector.get_explain_command()
        # Then
        assert cmd == expected

    @staticmethod
    def test_get_explain_command_analyze(connector: sqlite.SQLiteConnector):
        # Given
        expected = connector.EXPLAIN_PREFIX
        # When
        cmd = connector.get_explain_command(analyze=True)
        # Then
        assert cmd == expected

    @staticmethod
    def test_get_explain_command_format(connector: sqlite.SQLiteConnector):
        # Given
        expected = connector.EXPLAIN_PREFIX
        # When
        cmd = connector.get_explain_command(analyze=True, format="json")
        # Then
        assert cmd == expected
