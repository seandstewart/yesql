import argparse
import collections
import importlib.util
import os.path
import pathlib
import sys
from types import ModuleType
from typing import Iterator

from yesql import stubgen


def run(args: argparse.Namespace):
    sources = args.source or ["."]
    stubbed = stub_package(*sources)
    if stubbed:
        stubbed_str = "\n\t- ".join(str(p) for p in stubbed)
        print(f"Done! Stubbed {len(stubbed)} modules:\n\n\t- {stubbed_str}")
        sys.exit(0)
    print("Couldn't locate any files to stub.")
    sys.exit(1)


def configure_parser(
    parser: argparse.ArgumentParser | None = None,
) -> argparse.ArgumentParser:
    parse = parser or argparse.ArgumentParser()
    parse.add_argument(
        "source",
        action="append",
        default=[],
        type=str,
    )
    return parse


def stub_packages(*source: str) -> list[pathlib.Path]:
    saved = []
    for src_file in source:
        stubbed = stub_package(src_file)
        saved.extend(stubbed)

    return saved


def stub_package(src_file: str) -> list[pathlib.Path]:
    # If we've been passed a src file as a module path, make it a file-path.
    if ismodpath(src_file):
        src_file = src_file.replace(".", os.path.sep)
    # Resolve the abspath of the src_file
    relative_src_path = pathlib.Path(src_file)
    if not relative_src_path.exists():
        return []

    src_path = relative_src_path.resolve()
    home = pathlib.Path.home()
    # Try stubbing out all modules under the srd,
    #   Walk back until we hit home if we have trouble importing the module(s).
    while src_path != home:
        try:
            saved = stub_path_package(src_path=src_path)
            return saved
        except ImportError:
            # Indicates we're in a partially-initialized package.
            #   Walk back one directory and try again.
            src_path = src_path.parent

    return []


def stub_path_package(src_path: pathlib.Path) -> list[pathlib.Path]:
    # Walk all modules and try to stub out the pyi file.
    saved = []
    for module in walk_modules(src_path):
        stubbed = stubgen.stubgen(module)
        if stubbed:
            saved.append(stubbed)

    return saved


def walk_modules(src: pathlib.Path) -> Iterator[ModuleType]:
    # Walk all located modules and import them.
    for py_file, module_name in walk_py_files(src):
        module = dynamic_import(module_name, py_file)
        sys.modules[module_name] = module
        yield module


def ismodpath(path: str) -> bool:
    """Basic heuristic to see if we've been fed a module path instead of file path."""
    return "/" not in path and "\\" not in path and path != "."


def walk_py_files(src: pathlib.Path) -> Iterator[tuple[pathlib.Path, str]]:
    # Walk the path at src and locate all py files using an adapted DFS
    #   Resolve all packages first, then all modules.
    #   This ensures the import system can resolve relative imports.
    pkgs, modules = sort_node_children(src, "")
    stack = collections.deque([*pkgs, *modules])
    seen = set()
    while stack:
        node, pkg = stack.popleft()
        # Prevent recursion (could happen with a symlink, however unlikely)
        if node in seen:
            continue
        seen.add(node)
        # We got one!
        if node.is_file():
            yield node, pkg
        # Nested package, fill in the stack.
        elif node.is_dir():
            # Eagerly add the module if it has an init.
            # This helps with relative imports.
            init_file = node / "__init__.py"
            if init_file.exists():
                sys.modules[node.name] = dynamic_import(pkg, init_file)
                seen.add(init_file)
            # Walk the python packages and files directly under this package.
            pkgs, modules = sort_node_children(node, pkg)
            # Initialize packages before modules by adding them to the top of the stack.
            stack.extendleft(pkgs)
            stack.extend(modules)


def sort_node_children(
    node: pathlib.Path,
    pkg: str,
) -> tuple[list[tuple[pathlib.Path, str]], list[tuple[pathlib.Path, str]]]:
    pkgs, modules = [], []
    for child in node.iterdir():
        modname = child.stem
        if pkg:
            modname = f"{pkg}.{modname}"
        if ispymodule(child):
            modules.append((child, modname))
        elif maybepypackage(child):
            pkgs.append((child, modname))
    return pkgs, modules


def ispymodule(path: pathlib.Path) -> bool:
    return path.is_file() and path.suffix == ".py" and path.stem != "__init__"


def maybepypackage(path: pathlib.Path) -> bool:
    return path.is_dir() and path.name != "__pycache__"


def dynamic_import(module_name: str, py_path: pathlib.Path) -> ModuleType:
    module_spec = importlib.util.spec_from_file_location(module_name, py_path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module
