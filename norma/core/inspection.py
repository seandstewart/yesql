from aiosql import types

__all__ = ("isbulk", "isscalar", "ismutate", "ispersist")


def isbulk(func: types.QueryFn) -> bool:
    """Whether this query function may return multiple records."""
    return func.operation in _BULK_QUERIES


def isscalar(func: types.QueryFn) -> bool:
    """Whether the return value of this query function is not represented by the model."""
    return func.operation in _SCALAR_QUERIES


def ismutate(func: types.QueryFn) -> bool:
    """Whether this function results in a mutation of data."""
    return func.operation in _MUTATE_QUERIES


def ispersist(func: types.QueryFn) -> bool:
    """Whether this query function represents a creation or update of data."""
    sql = func.sql.lower()
    return "insert" in sql or "update" in sql


_SCALAR_QUERIES = {
    types.SQLOperationType.SELECT_VALUE,
    types.SQLOperationType.INSERT_UPDATE_DELETE,
    types.SQLOperationType.INSERT_UPDATE_DELETE_MANY,
    types.SQLOperationType.SCRIPT,
}
_BULK_QUERIES = {
    types.SQLOperationType.SELECT,
    types.SQLOperationType.INSERT_UPDATE_DELETE_MANY,
}
_MUTATE_QUERIES = {
    types.SQLOperationType.INSERT_UPDATE_DELETE,
    types.SQLOperationType.INSERT_UPDATE_DELETE_MANY,
    types.SQLOperationType.SCRIPT,
}
