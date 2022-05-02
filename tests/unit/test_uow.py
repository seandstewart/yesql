from __future__ import annotations

import dataclasses
import inspect
from unittest import mock

import pytest

from yesql import uow
from yesql.core import parse, types
from yesql.core.drivers import base


@dataclasses.dataclass
class DummyModel:
    attrib: str
    id: int = None


class DummyStatement(uow.Statement[DummyModel]):
    def execute(
        self,
        *args,
        instance: DummyModel = None,
        connection: types.ConnectionT = None,
        timeout: float = 10,
        transaction: bool = True,
        rollback: bool = False,
        serializer: base.SerializerT | None = None,
        **kwargs,
    ):
        return mock.call(
            *args,
            instance=instance,
            connection=connection,
            timeout=timeout,
            transaction=transaction,
            rollback=rollback,
            serializer=serializer,
            **kwargs,
        )


@pytest.fixture
def datum() -> parse.QueryDatum:
    return parse.QueryDatum(
        name="dummy",
        doc="",
        sql="select 1",
        signature=inspect.Signature([]),
        modifier="raw",
        remapping=None,
    )


@pytest.fixture
def middleware():
    return mock.MagicMock(spec=types.MiddlewareMethodProtocolT)


@pytest.fixture
def statement(datum, executor, middleware) -> DummyStatement:
    return DummyStatement(query=datum, executor=executor, middleware=middleware)


class TestCoreStatement:
    @staticmethod
    def test_middleware_setter_null(statement):
        # When
        statement.middleware = None
        # Then
        assert statement.__call__ == statement.execute

    @staticmethod
    def test_middleware_setter(statement, middleware):
        # Given
        statement.middleware = None
        # When
        statement.middleware = middleware
        # Then
        assert statement.__call__ == statement.execute_middleware

    @staticmethod
    def test_middleware_deleter(statement):
        # When
        del statement.middleware
        # Then
        assert statement.__call__ == statement.execute and statement._middleware is None

    @pytest.mark.parametrize(
        argnames="instance,serializer,expected_args,expected_kwargs",
        argvalues=[
            (DummyModel("data"), None, (), {"attrib": "data", "id": None}),
            (DummyModel("data"), dataclasses.astuple, ("data", None), {}),
            (None, dataclasses.astuple, (), {}),
        ],
        ids=["default", "custom", "no-instance"],
    )
    def test_serialize_instance(
        self, statement, instance, serializer, expected_args, expected_kwargs
    ):
        # When
        args, kwargs = statement._serialize_instance(
            instance=instance,
            serializer=serializer,
            args=(),
            kwargs={},
        )
        # Then
        assert (args, kwargs) == (expected_args, expected_kwargs)

    @pytest.mark.parametrize(
        argnames="instances,serializer,expected_params",
        argvalues=[
            ([DummyModel("data")], None, [{"attrib": "data", "id": None}]),
            ([DummyModel("data")], dataclasses.astuple, [("data", None)]),
            ([], dataclasses.astuple, []),
        ],
        ids=["default", "custom", "no-instances"],
    )
    def test_serialize_instances(
        self, statement, instances, serializer, expected_params
    ):
        # When
        params = statement._serialize_instances(
            instances=instances,
            serializer=serializer,
            params=[],
        )
        # Then
        assert params == expected_params


def test_uow_execute(uow_case):
    # Given
    unit, kwargs, exec_method, expected_call = uow_case
    # When
    unit.execute(**kwargs)
    # Then
    assert exec_method.call_args == expected_call


@pytest.fixture(
    params=[
        "many-default",
        "many-instance",
        "many-deserializer",
        "many-no-coerce",
        "one-default",
        "one-instance",
        "one-deserializer",
        "one-no-coerce",
        "many_cursor-default",
        "many_cursor-instance",
        "raw-default",
        "raw-instance",
        "raw_cursor-default",
        "raw_cursor-instance",
        "scalar-default",
        "scalar-instance",
        "affected-default",
        "affected-instance",
        "multi_cursor-default",
        "multi_cursor-instance",
        "multi-default",
        "multi-instance",
        "multi-deserializer",
        "multi-no-coerce",
    ]
)
def uow_case(request, executor, datum):
    mod, case = request.param.split("-", maxsplit=1)
    # if mod == "many":
    unit_cls = _MOD_TO_UOW_CLS[mod]
    unit = unit_cls(
        query=datum,
        executor=executor,
    )
    exec_method = getattr(executor, mod)
    deser = unit.serdes.deserializer if mod == "one" else unit.serdes.bulk_deserializer
    # Multi- uow have a slightly different signature.
    if mod.startswith("multi"):
        kwargs, call = _uow_multi_case(case, unit, datum)

    elif case == "instance":
        instance = DummyModel("value", 1)
        kwargs = {"instance": instance}
        call = mock.call(
            datum,
            connection=None,
            timeout=mock.ANY,
            transaction=mock.ANY,
            rollback=mock.ANY,
            deserializer=deser,
            attrib=instance.attrib,
            id=instance.id,
        )

    elif case == "deserializer":
        deser = lambda x: x  # noqa: E731
        kwargs = {"deserializer": deser}
        call = mock.call(
            datum,
            connection=None,
            timeout=mock.ANY,
            transaction=mock.ANY,
            rollback=mock.ANY,
            deserializer=deser,
        )

    elif case == "no-coerce":
        kwargs = {"coerce": False}
        call = mock.call(
            datum,
            connection=None,
            timeout=mock.ANY,
            transaction=mock.ANY,
            rollback=mock.ANY,
            deserializer=None,
        )

    else:
        kwargs = {}
        call = mock.call(
            datum,
            connection=None,
            timeout=mock.ANY,
            transaction=mock.ANY,
            rollback=mock.ANY,
            deserializer=deser,
        )
    if mod not in {"many", "one", "multi"}:
        call.kwargs.pop("deserializer", None)
        call.kwargs.pop("returns", None)

    if mod.endswith("cursor"):
        object.__setattr__(datum, "name", datum.name + "_cursor")

    return unit, kwargs, exec_method, call


def _uow_multi_case(case: str, unit: uow.Statement, datum: parse.QueryDatum):
    if case == "instance":
        instance = DummyModel("value", 1)
        kwargs = {"instances": [instance]}
        call = mock.call(
            datum,
            params=[{"attrib": instance.attrib, "id": instance.id}],
            connection=None,
            timeout=mock.ANY,
            transaction=mock.ANY,
            rollback=mock.ANY,
            returns=mock.ANY,
            deserializer=unit.serdes.bulk_deserializer,
        )
        return kwargs, call

    if case == "deserializer":
        deser = lambda x: x  # noqa: E731
        kwargs = {"deserializer": deser}
        call = mock.call(
            datum,
            params=(),
            connection=None,
            timeout=mock.ANY,
            transaction=mock.ANY,
            rollback=mock.ANY,
            returns=mock.ANY,
            deserializer=deser,
        )
        return kwargs, call

    if case == "no-coerce":
        kwargs = {"coerce": False}
        call = mock.call(
            datum,
            params=(),
            connection=None,
            timeout=mock.ANY,
            transaction=mock.ANY,
            rollback=mock.ANY,
            returns=mock.ANY,
            deserializer=None,
        )
        return kwargs, call

    kwargs = {}
    call = mock.call(
        datum,
        params=(),
        connection=None,
        timeout=mock.ANY,
        transaction=mock.ANY,
        rollback=mock.ANY,
        returns=mock.ANY,
        deserializer=unit.serdes.bulk_deserializer,
    )

    return kwargs, call


_MOD_TO_UOW_CLS = {
    "many": uow.Many,
    "many_cursor": uow.ManyCursor,
    "raw": uow.Raw,
    "raw_cursor": uow.RawCursor,
    "one": uow.One,
    "scalar": uow.Scalar,
    "multi": uow.Multi,
    "multi_cursor": uow.MultiCursor,
    "affected": uow.Affected,
}
