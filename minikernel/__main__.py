"""入口: python -m minikernel -f connection.json"""
from __future__ import annotations

import argparse
import logging
import sys

from .connection import load_connection_file
from .kernel import Kernel
from .supervisor import Supervisor


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="minikernel", description="纯 Python Jupyter 协议 5.3 后端内核")
    parser.add_argument("-f", "--connection-file", required=True, help="Jupyter 连接 JSON 路径")
    parser.add_argument("-v", "--verbose", action="store_true", help="输出调试日志")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="[minikernel %(levelname)s %(threadName)s] %(message)s",
        stream=sys.stderr,
    )
    conn = load_connection_file(args.connection_file)
    kernel = Kernel(conn)
    Supervisor(kernel).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
