from __future__ import annotations

import collections
import dataclasses
import functools
import inspect
import pathlib
import re
import warnings
from typing import TYPE_CHECKING, Deque, Final, Literal, cast

import sqlparse
import typic

if TYPE_CHECKING:
    from yesql.core.drivers import SupportedDriversT


__all__ = (
    "parse",
    "AFFECTED",
    "MANY",
    "MULTI",
    "ONE",
    "SCALAR",
    "RAW",
    "ModifierT",
    "QueryPackage",
    "QueryDatum",
    "QueryModule",
)


@functools.cache
def parse(
    queries: str | pathlib.Path,
    *,
    driver: SupportedDriversT,
    modname: str | None = None,
) -> QueryPackage:
    """Parse a string or path, returning a QueryPackage, for building a query library."""

    # If we have a raw string or a single module, create a package from that.
    if isinstance(queries, str) or queries.is_file():
        if isinstance(queries, pathlib.Path):
            path = queries.parent
            modname = modname or queries.stem
        else:
            path = pathlib.Path.cwd()
            modname = "<locals>"
        module = parse_module(queries=queries, modname=modname, driver=driver)
        return QueryPackage(name=modname, path=path, modules={module.name: module})
    # Otherwise, traverse the package with DFS, building the query tree.
    # Create the root package.
    package = QueryPackage(name=modname or queries.stem, modules={}, path=queries)
    stack: Deque[QueryPackage] = collections.deque([package])
    while stack:
        pkg = stack.popleft()
        # Traverse the directory, looking for modules to parse into QueryDatum
        for child in pkg.path.iterdir():
            # If we found a directory, add it to the stack and attach it to the parent.
            if child.is_dir():
                cpkg = QueryPackage(name=child.stem, modules={}, path=child)
                stack.append(cpkg)
                pkg.packages[cpkg.name] = cpkg
            # Otherwise, parse the module and attach it to the package.
            module = parse_module(queries=child, modname=child.stem, driver=driver)
            pkg.modules[module.name] = module

    return package


def parse_module(
    *, queries: str | pathlib.Path, modname: str, driver: SupportedDriversT
) -> QueryModule:
    if isinstance(queries, str):
        data = {
            datum.name: datum
            for statement in sqlparse.parse(queries)
            if (datum := get_query_datum(statement, driver=driver))
        }
        return QueryModule(modname, queries=data, path=pathlib.Path.cwd())
    with queries.open() as file:
        data = {
            d.name: d
            for statement in sqlparse.parsestream(file)
            if (d := get_query_datum(statement, driver=driver))
        }
        return QueryModule(modname, queries=data, path=queries)


def get_query_datum(
    statement: sqlparse.sql.Statement, driver: SupportedDriversT
) -> QueryDatum | None:
    lead, doc, start = get_preamble(statement)
    if not lead:
        return None

    name, modifier = get_funcop(lead)
    if not name:
        return None

    sql, sig, modifier, remapping = process_sql(
        statement,
        start,
        modifier,
        driver,
    )
    return QueryDatum(
        name=name,
        doc=doc,
        sql=sql,
        signature=sig,
        modifier=modifier,
        remapping=remapping,
    )


def get_preamble(statement: sqlparse.sql.Statement) -> tuple[str, str, int]:
    lead, docs, i = "", [], 0
    for i, token in enumerate(statement.tokens):
        # Stop when we hit the first part of the actual SQL statement.
        if token.is_keyword:
            break
        # Skip any newline or whitespace.
        if not isinstance(token, sqlparse.sql.Comment):
            continue
        token, ismultiline = next(
            (
                (i, multi)
                for i in token
                if (multi := i.ttype == sqlparse.tokens.Comment.Multiline)
            ),
            (token, False),
        )
        # Process multline docs
        if ismultiline:
            comments = token.value.rstrip("/**").lstrip("**/").strip().splitlines()
            if not lead:
                lead, comments = comments[0], comments[1:]
            docs.extend(comments)
            continue
        comment = _clean_comment(token.value)
        if not lead:
            lead = comment
            continue

        docs.append(comment)

    return lead, "\n".join(docs), i


def get_funcop(lead: str) -> tuple[str | None, ModifierT]:
    """Extract the name of the function and the fetch-modifier."""

    match = FUNC_PATTERN.match(lead)
    name = match.group("name")
    modifier = match.group("modifier") or MANY
    if modifier not in MODIFIERS:
        if name not in _SHORT_TO_LONG:
            warnings.warn(
                f"Unrecognized query modifier: {modifier!r}. "
                f"Recognized modifiers are: {(*MODIFIERS,)}. "
                f"Defaulting to {MANY!r}.",
                stacklevel=10,
            )
            modifier = MANY
        else:
            modifier = _SHORT_TO_LONG[modifier]
    return name, cast(ModifierT, modifier)


def process_sql(
    statement: sqlparse.sql.Statement,
    start: int,
    modifier: ModifierT,
    driver: SupportedDriversT,
) -> tuple[str, inspect.Signature, ModifierT, dict | None] | None:
    op = statement.get_type()
    if op is None:
        return None
    posargs, kwdargs = {}, {}
    for token in statement.tokens[start:]:
        pos, kwd = _gather_parameters(token)
        posargs.update(pos)
        kwdargs.update(kwd)
        continue

    sig = inspect.Signature([*posargs.values(), *kwdargs.values()])
    sql, remapping = _normalize_parameters(statement, driver, posargs, kwdargs)
    return sql, sig, modifier, remapping


def _gather_parameters(
    token: sqlparse.tokens.Token,
) -> tuple[dict[str, inspect.Parameter], dict[str, inspect.Parameter]]:
    kwdargs = {}
    posargs = {}
    argnum = 0
    for token in token.flatten():
        if not token.ttype == sqlparse.tokens.Name.Placeholder:
            continue
        name = token.value[2:-2] if token.value.startswith("%(") else token.value[1:]
        if not name:
            argnum += 1
            name = str(argnum)
        if name.isdigit():
            name = f"arg{name}"
            kind = inspect.Parameter.POSITIONAL_ONLY
            posargs[token.value] = inspect.Parameter(name, kind)
            continue
        kwdargs[token.value] = inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY)
    return posargs, kwdargs


def _normalize_parameters(
    statement: sqlparse.sql.Statement,
    driver: SupportedDriversT,
    posargs: dict[str, inspect.Parameter],
    kwdargs: dict[str, inspect.Parameter],
) -> tuple[str, dict[str, int] | None]:
    sql, remapping = str(statement), None
    if driver == "asyncpg" and kwdargs:
        remapping = {}
        start = 1
        if posargs:
            start = [int(a.replace("arg", "")) for a in posargs][-1]
        for i, (name, param) in enumerate(kwdargs.items(), start=start):
            sql = re.sub(name, f"${i}", sql)
            remapping[param.name] = i
    elif driver == "psycopg":
        for name, param in kwdargs.items():
            sql = re.sub(name, f"%({param.name})s", sql)
    return sql, remapping


def _clean_comment(comment: str) -> str:
    return comment.lstrip(_PRE).lstrip()


_PRE = "--"

FUNC_PATTERN = re.compile(
    # The name of the query
    r":name\s+(?P<name>(\w+([-_])?\w+)+)"
    # The operation modifier
    r"(\s+:(?P<modifier>[*^$!#~]|many|one|scalar|multi|affected|raw))?"
)

MANY: Final = "many"
ONE: Final = "one"
SCALAR: Final = "scalar"
MULTI: Final = "multi"
AFFECTED: Final = "affected"
RAW: Final = "raw"
MODIFIERS = frozenset((MANY, ONE, SCALAR, MULTI, AFFECTED, RAW))
ModifierT = Literal["many", "one", "scalar", "multi", "affected", "raw"]
_SHORT_TO_LONG: dict[str, ModifierT] = {
    "*": MANY,
    "^": ONE,
    "$": SCALAR,
    "!": MULTI,
    "#": AFFECTED,
    "~": RAW,
}


@typic.slotted(dict=False, weakref=True)
@dataclasses.dataclass(frozen=True)
class QueryDatum:
    name: str
    doc: str
    sql: str
    signature: inspect.Signature
    modifier: ModifierT
    remapping: dict | None = dataclasses.field(default=None, hash=False)


@typic.slotted(dict=False, weakref=True)
@dataclasses.dataclass
class QueryModule:
    name: str
    path: pathlib.Path
    queries: dict[str, QueryDatum]

    def __getattr__(self, item: str) -> QueryDatum:
        if item not in self.queries:
            raise AttributeError(f"{item!r}")
        return self.queries[item]


@typic.slotted(dict=False, weakref=True)
@dataclasses.dataclass
class QueryPackage:
    name: str
    path: pathlib.Path
    modules: dict[str, QueryModule] = dataclasses.field(default_factory=dict)
    packages: dict[str, QueryPackage] = dataclasses.field(default_factory=dict)

    def __getattr__(self, item: str) -> QueryModule | QueryPackage:
        if item in self.packages:
            return self.packages[item]
        if item in self.modules:
            return self.modules[item]
        raise AttributeError(f"{item!r}")
