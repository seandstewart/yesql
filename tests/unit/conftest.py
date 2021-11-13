from unittest import mock

import pytest


@pytest.fixture(scope="package", autouse=True)
def MockAsyncPGConnection():
    with mock.patch("asyncpg.Connection", autospec=True) as mconn:
        with mock.patch("asyncpg.connect", autospec=True) as mmconn:
            mmconn.return_value = mconn.return_value
            yield mconn


@pytest.fixture(scope="package", autouse=True)
def MockAsyncPGTransaction(MockAsyncPGConnection):
    with mock.patch(
        "asyncpg.connection.transaction.Transaction", autospec=True
    ) as mtran:
        MockAsyncPGConnection.return_value.transaction.return_value = mtran.return_value
        yield mtran


@pytest.fixture(scope="package", autouse=True)
def MockAsyncPGPool(MockAsyncPGConnection):
    conn = MockAsyncPGConnection.return_value
    with mock.patch("asyncpg.Pool", autospec=True) as mpool:
        inst = mpool.return_value
        inst.__aenter__ = mock.AsyncMock(return_value=inst)
        inst.acquire.return_value.__aenter__.return_value = conn
        with mock.patch("asyncpg.create_pool", autospec=True) as mmpool:
            mmpool.return_value = inst
            yield mpool


@pytest.fixture(scope="package", autouse=True)
def MockAIOSQLiteConnection():
    with mock.patch("aiosqlite.Connection", autospec=True) as mconn:
        inst = mconn.return_value
        inst.__aenter__.return_value = inst
        inst.execute.side_effect = mock.AsyncMock()
        with mock.patch("aiosqlite.connect", autospec=True) as mmconn:
            mmconn.return_value = inst
            yield mconn


@pytest.fixture(scope="package", autouse=True)
def MockPsycoPGConnection():
    with mock.patch("psycopg.Connection", autospec=True) as mconn:
        with mock.patch("psycopg.connect", autospec=True) as mmconn:
            mmconn.return_value = mconn.return_value
            yield mconn


@pytest.fixture(scope="package", autouse=True)
def MockPsycoPGTransaction(MockPsycoPGConnection):
    with mock.patch("psycopg.transaction.Transaction", autospec=True) as mtran:
        MockPsycoPGConnection.return_value.transaction.return_value = mtran.return_value
        yield mtran


@pytest.fixture(scope="package", autouse=True)
def MockPsycoPGPool(MockPsycoPGConnection):
    conn = MockPsycoPGConnection.return_value
    with mock.patch("psycopg_pool.ConnectionPool", autospec=True) as mpool:
        inst = mpool.return_value
        inst.kwargs = {}
        inst.__enter__.return_value = inst
        inst.connection.return_value.__enter__.return_value = conn
        yield mpool


@pytest.fixture(scope="package", autouse=True)
def MockSQLiteConnection():
    with mock.patch("sqlite3.Connection", autospec=True) as mconn:
        inst = mconn.return_value
        inst.__enter__.return_value = inst
        with mock.patch("sqlite3.connect", autospec=True) as mmconn:
            mmconn.return_value = inst
            yield mconn
