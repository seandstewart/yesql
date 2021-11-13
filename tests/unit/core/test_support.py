import dataclasses
import inspect
from typing import Iterable
from unittest import mock

import pytest
import typic

import norma

pytestmark = pytest.mark.asyncio


@dataclasses.dataclass
class Foo:
    bar: int


class TestableError(Exception):
    ...


class TestableConnector:
    TRANSIENT = (TestableError,)


class TestableService:
    protocol = typic.protocol(Foo, is_optional=True)
    bulk_protocol = typic.protocol(Iterable[Foo])
    connector = TestableConnector()


class TestCoerceable:
    @staticmethod
    @pytest.mark.parametrize(
        argnames="func",
        argvalues=[
            mock.MagicMock(return_value={"bar": 1}),
            mock.AsyncMock(return_value={"bar": 1}),
        ],
    )
    async def test_coerceable_coerce(func):
        # Given
        self = TestableService()
        # When
        wrapped = norma.support.coerceable(func)
        result = wrapped(self)
        if inspect.isawaitable(result):
            result = await result
        # Then
        assert result == Foo(**func.return_value)

    @staticmethod
    @pytest.mark.parametrize(
        argnames="func",
        argvalues=[
            mock.MagicMock(return_value={"bar": 1}),
            mock.AsyncMock(return_value={"bar": 1}),
        ],
    )
    async def test_coerceable_no_coerce(func):
        # Given
        self = TestableService()
        # When
        wrapped = norma.support.coerceable(func)
        result = wrapped(self, coerce=False)
        if inspect.isawaitable(result):
            result = await result
        # Then
        assert result == func.return_value

    @staticmethod
    @pytest.mark.parametrize(
        argnames="func",
        argvalues=[
            mock.MagicMock(return_value=[{"bar": 1}]),
            mock.AsyncMock(return_value=[{"bar": 1}]),
        ],
    )
    async def test_bulk_coerceable_coerce(func):
        # Given
        self = TestableService()
        # When
        wrapped = norma.support.coerceable(func, bulk=True)
        result = wrapped(self)
        if inspect.isawaitable(result):
            result = await result
        # Then
        assert result == [Foo(**r) for r in func.return_value]

    @staticmethod
    @pytest.mark.parametrize(
        argnames="func",
        argvalues=[
            mock.MagicMock(return_value=[{"bar": 1}]),
            mock.AsyncMock(return_value=[{"bar": 1}]),
        ],
    )
    async def test_bulk_coerceable_no_coerce(func):
        # Given
        self = TestableService()
        # When
        wrapped = norma.support.coerceable(func, bulk=True)
        result = wrapped(self, coerce=False)
        if inspect.isawaitable(result):
            result = await result
        # Then
        assert result == func.return_value


class TestRetry:
    @staticmethod
    @pytest.mark.parametrize(
        argnames="func",
        argvalues=[
            mock.MagicMock(return_value=[{"bar": 1}]),
            mock.AsyncMock(return_value=[{"bar": 1}]),
        ],
    )
    async def test_retry_passthru(func):
        # Given
        self = TestableService()
        # When
        wrapped = norma.support.retry(func)
        result = wrapped(self)
        if inspect.isawaitable(result):
            result = await result
        # Then
        assert result == func.return_value

    @staticmethod
    @pytest.mark.parametrize(
        argnames="func",
        argvalues=[
            mock.MagicMock(side_effect=[TestableError, TestableError, {"bar": 1}]),
            mock.AsyncMock(side_effect=[TestableError, TestableError, {"bar": 1}]),
        ],
    )
    async def test_retry_succeeds(func):
        # Given
        self = TestableService()
        # When
        wrapped = norma.support.retry(func)
        result = wrapped(self)
        if inspect.isawaitable(result):
            result = await result
        # Then
        assert result == {"bar": 1}

    @staticmethod
    @pytest.mark.parametrize(
        argnames="func",
        argvalues=[
            mock.MagicMock(side_effect=[TestableError, TestableError]),
            mock.AsyncMock(side_effect=[TestableError, TestableError]),
        ],
    )
    async def test_retry_reraise(func):
        # Given
        self = TestableService()
        # When
        wrapped = norma.support.retry(func, retries=1)
        # Then
        with pytest.raises(TestableError):
            result = wrapped(self)
            if inspect.isawaitable(result):
                await result
