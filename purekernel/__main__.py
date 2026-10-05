"""Process entry point: ``python -m purekernel -f connection.json``.

This module is the process supervisor: it parses arguments, loads the
connection file, installs logging, runs the kernel and translates its
lifecycle into process exit codes.
"""

from __future__ import annotations

import argparse
import logging
import sys

from .connection import ConnectionInfo
from .kernel import Kernel

log = logging.getLogger("purekernel")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="purekernel",
        description="A pure-Python Jupyter protocol 5.3 kernel backend.",
    )
    parser.add_argument(
        "-f",
        "--connection-file",
        required=True,
        metavar="PATH",
        help="path to the Jupyter connection JSON file",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enable debug logging",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stderr,
    )

    try:
        conn = ConnectionInfo.from_file(args.connection_file)
    except (OSError, ValueError, KeyError) as exc:
        parser.exit(2, f"purekernel: cannot load connection file: {exc}\n")

    kernel = Kernel(conn)
    try:
        kernel.run()
    except Exception:
        log.exception("fatal kernel error")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
