import importlib
import inspect
import pathlib
import string
import textwrap
from types import ModuleType

from yesql.repository import BaseQueryRepository


def stubgen(module: str | ModuleType):
    if isinstance(module, str):
        module = get_module(module)

    stub = get_stub_module(module)
    module_path = pathlib.Path(module.__file__).resolve()
    stub_path = module_path.with_suffix(".pyi")
    stub_path.write_text(stub)
    return stub_path


def get_module(string) -> ModuleType:
    path = string.rsplit(".", maxsplit=1)
    target, package = path[-1], None
    if len(path) > 1:
        target, package = path
    module = importlib.import_module(name=target, package=package)
    return module


def is_repository(o):
    return inspect.isclass(o) and issubclass(o, BaseQueryRepository)


def get_stub_module(module: ModuleType) -> str:
    module_def = inspect.getsource(module)
    replacements: list[tuple[str, str]] = []
    repo: type[BaseQueryRepository]
    for name, repo in inspect.getmembers(module, is_repository):
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
        return_type = repo.model.__name__
        if statement.query.modifier in {"many", "multi"}:
            return_type = f"list[{repo.model.__name__}]"
        elif statement.query.modifier in {"scalar", "raw"}:
            return_type = "Any"
        elif statement.query.modifier == "affected":
            return_type = "int"
        instance_params = {**inspect.signature(statement.execute).parameters}
        instance_params.pop("coerce", None)
        instance_params.pop("args", None)
        instance_params.pop("kwargs", None)
        instance_params.pop("_", None)
        query_params = {**statement.query.signature.parameters, **instance_params}
        query_params.pop("instance", None)
        query_params.pop("instances", None)
        coerce_false = inspect.Parameter(
            "coerce", inspect.Parameter.KEYWORD_ONLY, annotation="Literal[False]"
        )
        coerce_true = inspect.Parameter(
            "coerce", inspect.Parameter.KEYWORD_ONLY, annotation="Literal[True]"
        )
        self_param = inspect.Parameter("self", kind=inspect.Parameter.POSITIONAL_ONLY)
        instance_sig = inspect.Signature(
            [self_param, *instance_params.values()], return_annotation=return_type
        )
        instance_sig_no_coerce = inspect.Signature(
            [self_param, *instance_params.values(), coerce_false],
            return_annotation="Any",
        )
        instance_sig_coerce = inspect.Signature(
            [self_param, *instance_params.values(), coerce_true],
            return_annotation=return_type,
        )
        query_sig = inspect.Signature(
            [self_param, *query_params.values()], return_annotation=return_type
        )
        query_sig_no_coerce = inspect.Signature(
            [self_param, *query_params.values(), coerce_false], return_annotation="Any"
        )
        query_sig_coerce = inspect.Signature(
            [self_param, *query_params.values(), coerce_true],
            return_annotation=return_type,
        )
        istr = method_template.substitute(
            method_name=method_name,
            method_sig=instance_sig,
            method_sig_coerce=instance_sig_coerce,
            method_sig_no_coerce=instance_sig_no_coerce,
            doc=statement.query.doc,
        )
        qstr = method_template.substitute(
            method_name=method_name,
            method_sig=query_sig,
            method_sig_coerce=query_sig_coerce,
            method_sig_no_coerce=query_sig_no_coerce,
            doc=statement.query.doc,
        )
        methods.extend((istr, qstr))

    return methods


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
