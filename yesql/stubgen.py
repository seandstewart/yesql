from __future__ import annotations

import importlib
import inspect
import pathlib
import string
import sys
import textwrap
from types import ModuleType
from typing import TypedDict

from yesql.core.parse import QueryDatum
from yesql.repository import BaseQueryRepository
from yesql.statement import Statement


def stubgen(module: str | ModuleType) -> pathlib.Path | None:
    if isinstance(module, str):
        module = get_module(module)

    stub = get_stub_module(module)
    if stub is None:
        return None

    module_path = pathlib.Path(module.__file__).resolve()
    stub_path = module_path.with_suffix(".pyi")
    stub_path.write_text(stub)
    return stub_path


def get_module(string) -> ModuleType:
    module = importlib.import_module(string)
    sys.modules[module.__name__] = module
    return module


def is_repository(o):
    return inspect.isclass(o) and issubclass(o, BaseQueryRepository)


def get_stub_module(module: ModuleType) -> str | None:
    try:
        module_def = inspect.getsource(module)
    except OSError:
        return None
    replacements: list[tuple[str, str]] = []
    repos: list[tuple[str, type[BaseQueryRepository]]] = inspect.getmembers(
        module, is_repository
    )
    if not repos:
        return None

    for name, repo in repos:
        try:
            class_def = inspect.getsource(repo)
        except (OSError, TypeError):
            # TODO
            continue

        methods: list[str] = get_stub_methods(repo)
        method_body = "".join(methods)
        class_def_stub = class_def + textwrap.indent(method_body, "    ")
        replacements.append((class_def, class_def_stub))

    module_def_stub = "import typing\n" + module_def
    for src, replacement in replacements:
        module_def_stub = module_def_stub.replace(src, replacement)
    return module_def_stub


def get_stub_methods(repo: type[BaseQueryRepository]) -> list[str]:
    methods: list[str] = []
    for method_name, statement in repo.__statements__.items():
        default_return, raw_return = get_return_types(
            modelname=repo.model.__name__, query=statement.query, isaio=repo.isaio
        )
        instance_params = get_instance_params(statement, repo)
        query_params = get_query_params(statement, instance_params)
        instance_sigs = get_signatures(
            *instance_params.values(),
            default_returns=default_return,
            raw_returns=raw_return,
            name=statement.query.name,
        )
        query_sigs = get_signatures(
            *query_params.values(),
            default_returns=default_return,
            raw_returns=raw_return,
            name=statement.query.name,
        )
        istr = method_template.substitute(
            method_name=method_name,
            doc=statement.query.doc,
            **instance_sigs,
        )
        qstr = method_template.substitute(
            method_name=method_name, doc=statement.query.doc, **query_sigs
        )
        methods.extend((istr, qstr))

    return methods


def get_return_types(modelname: str, query: QueryDatum, isaio: bool) -> tuple[str, str]:
    if query.name.endswith("cursor"):
        wrapper = "typing.AsyncContextManager" if isaio else "typing.ContextManager"
        rtype = f"{wrapper}[yesql.types.CursorT]"
        return rtype, rtype
    default = modelname
    raw = "typing.Any"
    if query.modifier in {"many", "multi"}:
        default = f"list[{modelname}]"
    elif query.modifier in {"scalar", "raw"}:
        default = "typing.Any"
    elif query.modifier == "affected":
        raw = default = "int"
    if isaio:
        default, raw = f"typing.Awaitable[{default}]", f"typing.Awaitable[{raw}]"
    return default, raw


def get_signatures(
    *params, default_returns: str, raw_returns: str, name: str
) -> StubSignatures:
    sig = inspect.Signature([self_param, *params], return_annotation=default_returns)
    if name.endswith("cursor"):
        return {
            "method_sig": sig,
            "method_sig_coerce": sig,
            "method_sig_no_coerce": sig,
        }

    sig_no_coerce = inspect.Signature(
        [self_param, *params, coerce_false],
        return_annotation=raw_returns,
    )
    sig_coerce = inspect.Signature(
        [self_param, *params, coerce_true],
        return_annotation=default_returns,
    )
    return {
        "method_sig": sig,
        "method_sig_coerce": sig_coerce,
        "method_sig_no_coerce": sig_no_coerce,
    }


def get_instance_params(
    statement: Statement, repo: type[BaseQueryRepository]
) -> dict[str, inspect.Parameter]:
    unprocessed = inspect.signature(statement.execute).parameters
    processed = {}
    for name, param in unprocessed.items():
        if name in ("coerce", "args", "kwargs", "_"):
            continue
        annotation = param.annotation
        if name == "instance":
            annotation = f"{repo.model.__name__} | None"
        elif name == "instances":
            annotation = f"typing.Sequence[{repo.model.__name__}]"
        elif name == "params":
            annotation = (
                "typing.Iterable[typing.Sequence | typing.Mapping[str, typing.Any]]"
            )
        elif name in ("connection", "serializer", "deserializer"):
            annotation = "yesql.types." + param.annotation.rsplit(".")[-1]
        processed[name] = param.replace(annotation=annotation)
    return processed


def get_query_params(
    statement: Statement, instance_params: dict[str, inspect.Parameter]
):
    query_params = {**statement.query.signature.parameters, **instance_params}
    query_params.pop("instance", None)
    query_params.pop("instances", None)
    return query_params


class StubSignatures(TypedDict):
    method_sig: inspect.Signature
    method_sig_coerce: inspect.Signature
    method_sig_no_coerce: inspect.Signature


coerce_false = inspect.Parameter(
    "coerce", inspect.Parameter.KEYWORD_ONLY, annotation="typing.Literal[False]"
)
coerce_true = inspect.Parameter(
    "coerce", inspect.Parameter.KEYWORD_ONLY, annotation="typing.Literal[True]"
)
self_param = inspect.Parameter("self", kind=inspect.Parameter.POSITIONAL_ONLY)


method_template_str = '''
@typing.overload
def ${method_name}${method_sig}:
    """${doc}
    """

@typing.overload
def ${method_name}${method_sig_coerce}:
    """${doc}
    """

@typing.overload
def ${method_name}${method_sig_no_coerce}:
    """${doc}
    """
'''
method_template = string.Template(method_template_str)
