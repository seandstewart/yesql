import inspect
from unittest import mock

import sqlparse.tokens

from yesql.core import parse


def test_clean_comment():
    # Given
    comment = "-- Comments 4 U "
    expected = "Comments 4 U"
    # When
    cleaned = parse._clean_comment(comment)
    # Then
    assert cleaned == expected


def test_split_comments():
    # Given
    comments = """
    /** I've got a lot to say.

    And I'm gonna say it. **/
    """
    expected = ["I've got a lot to say.", "", "And I'm gonna say it."]
    # When
    split = parse._split_comments(comments)
    # Then
    assert split == expected


def test_normalize_parameters_asyncpg():
    # Given
    statement = "select * from foo where blar=$1, bar=:bar"
    posarg = inspect.Parameter(
        "arg1",
        kind=inspect.Parameter.POSITIONAL_ONLY,
    )
    kwdarg = inspect.Parameter(
        "bar",
        kind=inspect.Parameter.KEYWORD_ONLY,
    )
    posargs = {"$1": posarg}
    kwdargs = {":bar": kwdarg}
    expected_sql = "select * from foo where blar=$1, bar=$2"
    expected_remapping = {kwdarg.name: 2}
    # When
    sql, remapping = parse._normalize_parameters(
        statement=statement,
        driver="asyncpg",
        posargs=posargs,
        kwdargs=kwdargs,
    )
    # Then
    assert (sql, remapping) == (expected_sql, expected_remapping)


def test_normalize_parameters_psycopg():
    # Given
    statement = "select * from foo where blar=:blar, bar=:bar"
    kwdargs = {
        ":bar": inspect.Parameter(
            "bar",
            kind=inspect.Parameter.KEYWORD_ONLY,
        ),
        ":blar": inspect.Parameter(
            "blar",
            kind=inspect.Parameter.KEYWORD_ONLY,
        ),
    }

    expected_sql = "select * from foo where blar=%(blar)s, bar=%(bar)s"
    expected_remapping = None
    # When
    sql, remapping = parse._normalize_parameters(
        statement=statement,
        driver="psycopg",
        posargs={},
        kwdargs=kwdargs,
    )
    # Then
    assert (sql, remapping) == (expected_sql, expected_remapping)


def test_normalize_parameters_no_kwdargs():
    # Given
    statement = "select * from foo where blar=$1, bar=$2"
    # When
    sql, remapping = parse._normalize_parameters(
        statement=statement, driver="asyncpg", posargs={}, kwdargs={}
    )
    # Then
    assert (sql, None) == (statement, None)


def test_gather_parameters():
    # Given
    token = _mock_token()
    ttype = sqlparse.tokens.Name.Placeholder
    filtered = _mock_token()
    params = {}
    expected_posargs = {}
    expected_kwdargs = {}
    for pname, pout, posarg in [
        ("$foo", "foo", False),
        ("%(bar)s", "bar", False),
        ("%()s", "arg1", True),
        (":2", "arg2", True),
    ]:

        params[pout] = (_mock_token(ttype=ttype, value=pname), posarg)
        if posarg:
            expected_posargs[pname] = inspect.Parameter(
                name=pout,
                kind=inspect.Parameter.POSITIONAL_ONLY,
            )
        else:
            expected_kwdargs[pname] = inspect.Parameter(
                name=pout, kind=inspect.Parameter.KEYWORD_ONLY
            )
    token.flatten.return_value = [filtered, *(t for t, p in params.values())]
    # When
    posargs, kwdargs = parse._gather_parameters(token)
    # Then
    assert (posargs, kwdargs) == (expected_posargs, expected_kwdargs)


def _mock_token(**overrides):
    m = mock.Mock(**overrides)
    return m
