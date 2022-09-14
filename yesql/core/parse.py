from __future__ import annotations

import collections
import dataclasses
import functools
import inspect
import pathlib
import re
import sys
import warnings
from typing import TYPE_CHECKING, Deque, Final, Literal, Optional, Tuple, cast

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


@functools.lru_cache(maxsize=None)
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
            modname = modname or "<locals>"
        module = parse_module(queries=queries, modname=modname, driver=driver)
        return QueryPackage(name=modname, path=path, modules={module.name: module})
    # Otherwise, traverse the package with BFS, building the query tree.
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
                continue
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

    processed: _ProcessedT = process_sql(statement, start, driver)
    if processed is None:
        return None

    sql, sig, remapping = processed
    return QueryDatum(
        name=name,
        doc=doc,
        sql=sql,
        signature=sig,
        modifier=modifier,
        remapping=remapping,
    )


def get_preamble(statement: sqlparse.sql.Statement) -> tuple[str, str, int]:
    docs: list[str]
    lead, docs, i = "", [], 0
    gen = _iter_comments(statement)
    i, lead = next(gen, (0, ""))
    if not lead:
        return "", "", 0
    docs.extend(c for (i, c) in gen)
    return lead, "\n".join(docs), i


def _iter_comments(statement: sqlparse.sql.Statement):
    for ix, token in enumerate(statement.tokens):
        if token.is_keyword:
            break
        # Skip any newline or whitespace.
        if not isinstance(token, sqlparse.sql.Comment):
            continue
        # Test whether we have a multiline comment.
        token, ismultiline = next(
            (
                (i, multi)
                for i in token
                if (multi := i.ttype == sqlparse.tokens.Comment.Multiline)
            ),
            (token, False),
        )
        # If we do, extract it and yield from that.
        if ismultiline:
            yield from (
                (ix, cs) for c in _split_comments(token.value) if (cs := c.strip())
            )
        # Otherwise, yield from the token group of single-line comments.
        else:
            yield from ((ix, c) for t in token.tokens if (c := _clean_comment(t.value)))


def get_funcop(lead: str) -> tuple[str | None, ModifierT]:
    """Extract the name of the function and the fetch-modifier."""

    match = FUNC_PATTERN.match(lead)
    if not match:
        return None, MANY
    name = match.group("name")
    modifier = match.group("modifier")
    if not modifier:
        warnings.warn(
            f"Unrecognized query modifier: {modifier!r}. "
            f"Recognized modifiers are: {(*MODIFIERS,)}. "
            f"Defaulting to {MANY!r}.",
            stacklevel=10,
        )
        modifier = MANY
    elif modifier in _SHORT_TO_LONG:
        modifier = _SHORT_TO_LONG[modifier]
    return name, cast(ModifierT, modifier)


def process_sql(
    statement: sqlparse.sql.Statement,
    start: int,
    driver: SupportedDriversT,
) -> _ProcessedT:
    op = statement.get_type()
    if op is None:
        return None
    posargs, kwdargs = {}, {}
    for token in statement.tokens[start:]:
        pos, kwd = _gather_parameters(token)
        posargs.update(pos)
        kwdargs.update(kwd)

    sig = inspect.Signature([*posargs.values(), *kwdargs.values()])
    sql, remapping = _normalize_parameters(str(statement), driver, posargs, kwdargs)
    return sql, sig, remapping


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
    statement: str,
    driver: SupportedDriversT,
    posargs: dict[str, inspect.Parameter],
    kwdargs: dict[str, inspect.Parameter],
) -> tuple[str, dict[str, int] | None]:
    sql, remapping = statement, None
    if not kwdargs:
        return sql, remapping

    if driver == "asyncpg":
        remapping = {}
        start = 1
        if posargs:
            start = [int(a.name.replace("arg", "")) for a in posargs.values()][-1] + 1
        for i, (name, param) in enumerate(kwdargs.items(), start=start):
            sql = _replace(name=name, replacement=f"${i}", sql=sql)
            remapping[param.name] = i
    elif driver == "psycopg":
        for name, param in kwdargs.items():
            sql = _replace(name=name, replacement=f"%({param.name})s", sql=sql)
    return sql, remapping


def _replace(*, name: str, replacement: str, sql: str) -> str:
    return re.sub(r"(?<!:)" + name, replacement, sql)


if sys.version_info >= (3, 9):

    def _clean_comment(comment: str) -> str:
        return comment.strip().removeprefix(_PRE).strip()

    def _split_comments(comment: str) -> list[str]:
        return [
            c.strip()
            for c in comment.strip()
            .removeprefix("/**")
            .removesuffix("**/")
            .strip()
            .splitlines()
        ]

else:

    def _clean_comment(comment: str) -> str:
        return comment.strip().strip(_PRE).strip()

    def _split_comments(comment: str) -> list[str]:
        return [
            c.strip()
            for c in comment.strip().strip("/**").rstrip("**/").strip().splitlines()
        ]


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
_ProcessedT = Optional[Tuple[str, inspect.Signature, Optional[dict]]]


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
