import dataclasses
import inspect
from typing import Any, NamedTuple
from unittest import mock

import pytest

import yesql

pytestmark = pytest.mark.asyncio


@dataclasses.dataclass
class Foo:
    bar: int


class TestableError(Exception):
    ...


class TestableExecutor:
    TRANSIENT = (TestableError,)


class Params(NamedTuple):
    isasync: bool
    return_value: Any = None
    side_effect: Any = None


@pytest.fixture
def func(request):
    isasync, return_value, side_effect = request.param
    mock_class = mock.AsyncMock if isasync else mock.MagicMock
    func = mock_class(return_value=return_value, side_effect=side_effect)
    with mock.patch("yesql.core.support._isasync", return_value=isasync):
        yield func


class TestRetry:
    @staticmethod
    @pytest.mark.parametrize(
        argnames="func",
        argvalues=[
            Params(False, return_value={"bar": 1}),
            Params(True, return_value={"bar": 1}),
        ],
        indirect=True,
        ids=["sync", "async"],
    )
    async def test_retry_passthru(func):
        # Given
        self = TestableExecutor()
        # When
        wrapped = yesql.support.retry(func)
        result = wrapped(self)
        if inspect.isawaitable(result):
            result = await result
        # Then
        assert result == func.return_value

    @staticmethod
    @pytest.mark.parametrize(
        argnames="func",
        argvalues=[
            Params(False, side_effect=[TestableError, TestableError, {"bar": 1}]),
            Params(True, side_effect=[TestableError, TestableError, {"bar": 1}]),
        ],
        indirect=True,
        ids=["sync", "async"],
    )
    async def test_retry_succeeds(func):
        # Given
        self = TestableExecutor()
        # When
        wrapped = yesql.support.retry(func)
        result = wrapped(self)
        if inspect.isawaitable(result):
            result = await result
        # Then
        assert result == {"bar": 1}

    @staticmethod
    @pytest.mark.parametrize(
        argnames="func",
        argvalues=[
            Params(False, side_effect=TestableError),
            Params(True, side_effect=TestableError),
        ],
        indirect=True,
        ids=["sync", "async"],
    )
    async def test_retry_reraise(func):
        # Given
        self = TestableExecutor()
        # When
        wrapped = yesql.support.retry(func, retries=1)
        # Then
        with pytest.raises(TestableError):
            result = wrapped(self)
            if inspect.isawaitable(result):
                await result


class TestRetryCursor:
    @staticmethod
    @pytest.fixture
    def func(request):
        isasync, return_value, side_effect = request.param
        func = mock.MagicMock()
        called = func.return_value
        target = called.__aenter__ if isasync else called.__enter__
        if side_effect:
            target.side_effect = side_effect
        else:
            target.return_value = return_value
        with mock.patch("yesql.core.support._isasync", return_value=isasync):
            yield func

    @pytest.mark.parametrize(
        argnames="func",
        argvalues=[
            Params(True, return_value=True),
            Params(True, side_effect=[TestableError, TestableError, True]),
            Params(False, return_value=True),
            Params(False, side_effect=[TestableError, TestableError, True]),
        ],
        indirect=True,
        ids=["async-success", "async-errors", "sync-success", "sync-errors"],
    )
    async def test_retry_cursor(self, func):
        # Given
        svc = TestableExecutor()
        # When
        wrapped = yesql.support.retry_cursor(func)
        result = await self._exhaust_mock(wrapped, svc)
        # Then
        assert result

    @pytest.mark.parametrize(
        argnames="func",
        argvalues=[
            Params(True, side_effect=TestableError),
            Params(False, side_effect=TestableError),
        ],
        indirect=True,
    )
    async def test_retry_cursor_reraise(self, func):
        # Given
        svc = TestableExecutor()
        # When
        wrapped = yesql.support.retry_cursor(func, retries=2)
        with pytest.raises(TestableError):
            await self._exhaust_mock(wrapped, svc)

    @staticmethod
    async def _exhaust_mock(m, s):
        called = m(s)
        if hasattr(called, "__aenter__"):
            async with m(s) as result:
                return result
        with m(s) as result:
            return result
