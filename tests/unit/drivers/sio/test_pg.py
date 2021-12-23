import concurrent.futures
import time
from unittest import mock

import psycopg
import pytest

from yesql.drivers.sio import postgres


@pytest.fixture(autouse=True)
def pool(MockPsycoPGPool):
    MockPsycoPGPool.reset_mock()
    pool = MockPsycoPGPool.return_value
    yield pool
    MockPsycoPGPool.reset_mock()


@pytest.fixture(autouse=True)
def connection(MockPsycoPGConnection):
    conn = MockPsycoPGConnection.return_value
    yield conn
    MockPsycoPGConnection.reset_mock()


@pytest.fixture(autouse=True)
def transaction(MockPsycoPGTransaction):
    tran = MockPsycoPGTransaction.return_value
    yield tran
    MockPsycoPGTransaction.reset_mock()


@pytest.fixture(scope="module")
def connector() -> postgres.PsycoPGConnector:
    connector = postgres.PsycoPGConnector()
    return connector


class TestPsycoPGConnector:
    @staticmethod
    def test_initialize(connector: postgres.PsycoPGConnector, pool):
        # Given
        connector.initialized = False
        connector.pool = None
        # When
        connector.initialize()
        # Then
        assert connector.initialized
        assert connector.pool is pool

    @staticmethod
    def test_initialize_done(connector: postgres.PsycoPGConnector, MockPsycoPGPool):
        # Given
        connector.initialized = True
        # When
        connector.initialize()
        # Then
        assert not MockPsycoPGPool.called

    @staticmethod
    def test_initialize_concurrent(
        connector: postgres.PsycoPGConnector, MockPsycoPGPool
    ):
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
        assert MockPsycoPGPool.call_count == 1

    @staticmethod
    def test_connection(connector: postgres.PsycoPGConnector):
        # When
        with connector.connection():
            ...

        # Then
        assert connector.pool.connection.called

    @staticmethod
    def test_connection_provided(
        connector: postgres.PsycoPGConnector, connection: psycopg.Connection
    ):
        # When
        with connector.connection(connection=connection):
            ...

        # Then
        assert not connector.pool.connection.called

    @staticmethod
    def test_transaction(connector: postgres.PsycoPGConnector, connection):
        # Given
        expected = mock.call(force_rollback=False, savepoint_name=None)
        # When
        with connector.transaction():
            ...
        # Then
        assert connection.transaction.call_args == expected

    @staticmethod
    def test_transaction_rollback(connector: postgres.PsycoPGConnector, connection):
        # Given
        expected = mock.call(force_rollback=True, savepoint_name=None)
        # When
        with connector.transaction(rollback=True):
            ...
        # Then
        assert connection.transaction.call_args == expected

    @staticmethod
    def test_close(connector: postgres.PsycoPGConnector):
        # When
        connector.close()
        # Then
        assert connector.pool.close.called

    @staticmethod
    def test_close_timeout(connector: postgres.PsycoPGConnector):
        # Given
        timeout = 0.001

        def timesout(*args, **kwargs):
            time.sleep(timeout * 2)

        connector.pool.close.side_effect = timesout
        # When
        connector.close(timeout=timeout)
        # Then
        assert connector.pool.close.called

    @staticmethod
    def test_open(connector: postgres.PsycoPGConnector):
        # Given
        connector.pool.closed = False
        # Then
        assert connector.open

    @staticmethod
    def test_get_explain_command(connector: postgres.PsycoPGConnector):
        # Given
        expected = connector.EXPLAIN_PREFIX
        # When
        cmd = connector.get_explain_command()
        # Then
        assert cmd == expected

    @staticmethod
    def test_get_explain_command_analyze(connector: postgres.PsycoPGConnector):
        # Given
        expected = f"{connector.EXPLAIN_PREFIX} (ANALYZE, )"
        # When
        cmd = connector.get_explain_command(analyze=True)
        # Then
        assert cmd == expected

    @staticmethod
    def test_get_explain_command_format(connector: postgres.PsycoPGConnector):
        # Given
        expected = f"{connector.EXPLAIN_PREFIX} (ANALYZE, FORMAT json)"
        # When
        cmd = connector.get_explain_command(analyze=True, format="json")
        # Then
        assert cmd == expected
