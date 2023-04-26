#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import types

from yesql.bin import stubgen


def get_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    for name, module in SUB_COMMANDS.items():
        # https://docs.python.org/3/library/argparse.html#sub-commands
        sub_parser = subparsers.add_parser(name)
        sub_parser.set_defaults(run=module.run)
        module.configure_parser(sub_parser)
    return parser


SUB_COMMANDS: dict[str, types.ModuleType] = {
    "stubgen": stubgen,
}


def run():
    parser = get_parser()
    args = parser.parse_args()
    if "run" not in vars(args):
        parser.print_help(sys.stderr)
        parser.exit(2)

    return args.run(args)


if __name__ == "__main__":
    run()
