from __future__ import annotations

import inspect
import pathlib
from unittest import mock

import pytest
import sqlparse
from sqlparse import sql, tokens

from yesql import parse

# region: simple helper functions


@pytest.mark.parametrize(
    argnames="comment,expected",
    argvalues=[
        ("-- foo", "foo"),
        ("-- foo ", "foo"),
        (" foo ", "foo"),
    ],
    ids=["basic", "trailing-space", "leading-space"],
)
def test__clean_comment(comment: str, expected: str):
    # When
    cleaned = parse._clean_comment(comment)
    # Then
    assert cleaned == expected


@pytest.mark.parametrize(
    argnames="token,expected_posargs,expected_kwdargs",
    argvalues=[
        (
            sql.Token(tokens.Name.Placeholder, ":foo"),
            {},
            {":foo": inspect.Parameter("foo", inspect.Parameter.KEYWORD_ONLY)},
        ),
        (
            sql.Token(tokens.Name.Placeholder, "$foo"),
            {},
            {"$foo": inspect.Parameter("foo", inspect.Parameter.KEYWORD_ONLY)},
        ),
        (
            sql.Token(tokens.Name.Placeholder, "%(foo)s"),
            {},
            {"%(foo)s": inspect.Parameter("foo", inspect.Parameter.KEYWORD_ONLY)},
        ),
        (
            sql.Token(tokens.Name.Placeholder, "%()s"),
            {"%()s": inspect.Parameter("arg1", inspect.Parameter.POSITIONAL_ONLY)},
            {},
        ),
        (
            sql.Token(tokens.Name.Placeholder, "%(2)s"),
            {"%(2)s": inspect.Parameter("arg2", inspect.Parameter.POSITIONAL_ONLY)},
            {},
        ),
        (
            sql.Token(tokens.Name.Placeholder, "$1"),
            {"$1": inspect.Parameter("arg1", inspect.Parameter.POSITIONAL_ONLY)},
            {},
        ),
    ],
    ids=[
        "colon-name",
        "dollar-name",
        "pyfmt-name",
        "pyfmt",
        "pyfmt-digit",
        "dollar-digit",
    ],
)
def test__gather_parameters(
    token: sqlparse.tokens.Token,
    expected_posargs: dict[str, inspect.Parameter],
    expected_kwdargs: dict[str, inspect.Parameter],
):
    # When
    posargs, kwdargs = parse._gather_parameters(token)
    # Then
    assert (posargs, kwdargs) == (expected_posargs, expected_kwdargs)


@pytest.mark.parametrize(
    argnames="sql,driver,posargs,kwdargs,expected_sql,expected_remapping",
    argvalues=[
        (
            "select :foo",
            "asyncpg",
            {},
            {":foo": inspect.Parameter("foo", inspect.Parameter.KEYWORD_ONLY)},
            "select $1",
            {"foo": 1},
        ),
        (
            "select $1",
            "asyncpg",
            {"$1": inspect.Parameter("arg1", inspect.Parameter.KEYWORD_ONLY)},
            {},
            "select $1",
            None,
        ),
        (
            "select $1, :foo",
            "asyncpg",
            {"$1": inspect.Parameter("arg1", inspect.Parameter.POSITIONAL_ONLY)},
            {":foo": inspect.Parameter("foo", inspect.Parameter.KEYWORD_ONLY)},
            "select $1, $2",
            {"foo": 2},
        ),
        (
            "select :foo",
            "psycopg",
            {},
            {":foo": inspect.Parameter("foo", inspect.Parameter.KEYWORD_ONLY)},
            "select %(foo)s",
            None,
        ),
    ],
    ids=[
        "asyncpg-named",
        "asyncpg-digit",
        "asyncpg-named-digit-mixed",
        "psycopg-named",
    ],
)
def test__normalize_parameters(
    sql: str,
    driver: str,
    posargs: dict,
    kwdargs: dict,
    expected_sql: str,
    expected_remapping: dict,
):
    # Given
    (statement,) = sqlparse.parse(sql)
    # When
    normalized, remapping = parse._normalize_parameters(
        statement=statement,
        driver=driver,
        posargs=posargs,
        kwdargs=kwdargs,
    )
    # Then
    assert (normalized, remapping) == (
        expected_sql,
        expected_remapping,
    )


# endregion
# region: complex helpers


@pytest.mark.parametrize(
    argnames="sql,driver,expected_sql,expected_signature,expected_remapping",
    argvalues=[
        (
            "select :foo",
            "asyncpg",
            "select $1",
            inspect.Signature(
                [inspect.Parameter("foo", inspect.Parameter.KEYWORD_ONLY)]
            ),
            {"foo": 1},
        ),
        (
            "select $1",
            "asyncpg",
            "select $1",
            inspect.Signature(
                [inspect.Parameter("arg1", inspect.Parameter.POSITIONAL_ONLY)]
            ),
            None,
        ),
        (
            "select $1, :foo",
            "asyncpg",
            "select $1, $2",
            inspect.Signature(
                [
                    inspect.Parameter("arg1", inspect.Parameter.POSITIONAL_ONLY),
                    inspect.Parameter("foo", inspect.Parameter.KEYWORD_ONLY),
                ]
            ),
            {"foo": 2},
        ),
        (
            "select :foo",
            "psycopg",
            "select %(foo)s",
            inspect.Signature(
                [
                    inspect.Parameter("foo", inspect.Parameter.KEYWORD_ONLY),
                ]
            ),
            None,
        ),
        (
            "select %(foo)s",
            "psycopg",
            "select %(foo)s",
            inspect.Signature(
                [
                    inspect.Parameter("foo", inspect.Parameter.KEYWORD_ONLY),
                ]
            ),
            None,
        ),
    ],
    ids=[
        "asyncpg-named",
        "asyncpg-digit",
        "asyncpg-named-digit-mixed",
        "psycopg-named",
        "psycopg-named-pyfmt",
    ],
)
def test_process_sql(
    sql: str,
    driver: parse.SupportedDriversT,
    expected_sql: str,
    expected_signature: inspect.Signature,
    expected_remapping: dict,
):
    # Given
    (statement,) = sqlparse.parse(sql)
    # When
    sql, sig, remapping = parse.process_sql(statement, 0, driver)
    # Then
    assert (sql, sig, remapping) == (
        expected_sql,
        expected_signature,
        expected_remapping,
    )


@pytest.mark.parametrize(
    argnames="mod,expected_modifier",
    argvalues=[
        *parse._SHORT_TO_LONG.items(),
        *zip(parse.MODIFIERS, parse.MODIFIERS),
        ("unknown", parse.MANY),
    ],
)
@pytest.mark.parametrize(
    argnames="name,expected_name",
    argvalues=[
        (":name name", "name"),
        (":name long-name", "long-name"),
    ],
)
def test_get_funcop(name: str, mod: str, expected_name: str, expected_modifier: str):
    # Given
    lead = f"{name} :{mod}"
    # When
    name, modifier = parse.get_funcop(lead)
    # Then
    assert (name, modifier) == (expected_name, expected_modifier)


def test_get_funcop_no_match():
    # Given
    lead = "something :wrong"
    # When
    name, modifier = parse.get_funcop(lead)
    # Then
    assert (name, modifier) == (None, parse.MANY)


_MULTILINE = """
/** :name foo :*

Frobnicate the fizz-bazz to extract foobar.
**/
SELECT * FROM foo.bar;
"""
_EXPECTED = (":name foo :*", "Frobnicate the fizz-bazz to extract foobar.")
_SINGLELINE = """
-- :name foo :*
-- Frobnicate the fizz-bazz to extract foobar.
SELECT * FROM foo.bar;
"""


@pytest.mark.parametrize(
    argnames="comments",
    argvalues=[_MULTILINE, _SINGLELINE],
    ids=["multiline", "singleline"],
)
@pytest.mark.parametrize(
    argnames="expected_lead,expected_comment", argvalues=[_EXPECTED], ids=["success"]
)
def test_get_preamble(comments, expected_lead, expected_comment):
    # Given
    (statement,) = sqlparse.parse(comments)
    # When
    lead, comment, ix = parse.get_preamble(statement)
    # Then
    assert (lead, comment, ix) == (expected_lead, expected_comment, mock.ANY)


@pytest.mark.parametrize(
    argnames="sql,driver,expected_datum",
    argvalues=[
        (
            "-- :name foo :^\nselect * from foo limit 1;",
            "asyncpg",
            parse.QueryDatum(
                name="foo",
                doc="",
                sql="-- :name foo :^\nselect * from foo limit 1;",
                signature=inspect.Signature([]),
                modifier=parse.ONE,
                remapping=None,
            ),
        ),
        ("select * from foo limit 1;", "asyncpg", None),
        ("-- nothing\nselect * from foo limit 1;", "asyncpg", None),
    ],
    ids=["success", "no-lead", "no-name"],
)
def test_get_query_datum(
    sql: str, driver: parse.SupportedDriversT, expected_datum: parse.QueryDatum
):
    # Given
    (statement,) = sqlparse.parse(sql)
    # When
    datum = parse.get_query_datum(statement, driver)
    # Then
    assert datum == expected_datum


# endregion
# region: high-level parsers


_FOO_QUERIES = pathlib.Path(__file__).parent.parent / "queries" / "foo" / "queries.sql"
_FOO_SQL = _FOO_QUERIES.read_text().strip()
_FOO_MODULE = parse.QueryModule(
    name="queries",
    path=_FOO_QUERIES,
    queries={
        "get": parse.QueryDatum(
            name="get",
            doc="Get a foo by id.",
            sql=_FOO_SQL.replace(":id", "$1"),
            signature=inspect.Signature(
                [inspect.Parameter(name="id", kind=inspect.Parameter.KEYWORD_ONLY)]
            ),
            modifier=parse.ONE,
            remapping={"id": 1},
        )
    },
)
_BAR_QUERIES = (
    pathlib.Path(__file__).parent.parent / "queries" / "foo" / "bar" / "queries.sql"
)
_BAR_SQL = _FOO_QUERIES.read_text().strip()
_BAR_MODULE = parse.QueryModule(
    name="queries",
    path=_BAR_QUERIES,
    queries={
        "get": parse.QueryDatum(
            name="get",
            doc="Get a foo by id.",
            sql=_FOO_SQL.replace(":id", "$1"),
            signature=inspect.Signature(
                [inspect.Parameter(name="id", kind=inspect.Parameter.KEYWORD_ONLY)]
            ),
            modifier=parse.ONE,
            remapping={"id": 1},
        )
    },
)
_TEXT_MODULE = parse.QueryModule(
    name="test",
    path=pathlib.Path.cwd(),
    queries={
        "foo": parse.QueryDatum(
            name="foo",
            doc="",
            sql="-- :name foo :^\nselect * from foo limit 1;",
            signature=inspect.Signature([]),
            modifier=parse.ONE,
            remapping=None,
        )
    },
)


@pytest.mark.parametrize(
    argnames="queries,modname,driver,expected_module",
    argvalues=[
        (_TEXT_MODULE.queries["foo"].sql, _TEXT_MODULE.name, "asyncpg", _TEXT_MODULE),
        (
            _FOO_QUERIES,
            _FOO_MODULE.name,
            "asyncpg",
            _FOO_MODULE,
        ),
    ],
    ids=["parse-string", "parse-path"],
)
def test_parse_module(
    queries: str | pathlib.Path,
    modname: str,
    driver: parse.SupportedDriversT,
    expected_module: parse.QueryModule,
):
    # When
    module = parse.parse_module(
        queries=queries,
        modname=modname,
        driver=driver,
    )
    # Then
    assert module == expected_module


@pytest.mark.parametrize(
    argnames="queries,modname,driver,expected_package",
    argvalues=[
        (
            _TEXT_MODULE.queries["foo"].sql,
            _TEXT_MODULE.name,
            "asyncpg",
            parse.QueryPackage(
                name=_TEXT_MODULE.name,
                path=pathlib.Path.cwd(),
                modules={_TEXT_MODULE.name: _TEXT_MODULE},
                packages={},
            ),
        ),
        (
            _FOO_QUERIES.parent,
            None,
            "asyncpg",
            parse.QueryPackage(
                name=_FOO_MODULE.path.parent.stem,
                path=_FOO_QUERIES.parent,
                modules={_FOO_MODULE.name: _FOO_MODULE},
                packages={
                    _BAR_MODULE.path.parent.stem: parse.QueryPackage(
                        name=_BAR_MODULE.path.parent.stem,
                        path=_BAR_MODULE.path.parent,
                        modules={_BAR_MODULE.name: _BAR_MODULE},
                        packages={},
                    )
                },
            ),
        ),
    ],
    ids=["parse-string", "parse-path"],
)
def test_parse(
    queries: str | pathlib.Path,
    modname: str | None,
    driver: parse.SupportedDriversT,
    expected_package: parse.QueryPackage,
):
    # When
    package = parse.parse(queries, driver=driver, modname=modname)
    # Then
    assert package == expected_package
