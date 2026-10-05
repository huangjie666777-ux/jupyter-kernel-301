"""Execution context: shared namespace, stdio redirection, tracebacks.

The kernel (:mod:`purekernel.kernel`) owns sockets and message routing;
this module owns everything that happens *inside* a cell:

* compiling the cell so a trailing bare expression is evaluated and its
  ``repr`` becomes the ``text/plain`` result;
* executing against one shared namespace so variables persist across
  cells;
* capturing ``sys.stdout``/``sys.stderr`` line-by-line so output is
  published to IOPub as it is produced;
* replacing ``sys.stdin`` so interactive input is refused immediately
  instead of hanging the kernel;
* formatting exceptions into ``(ename, evalue, traceback)`` triples with
  kernel-internal frames trimmed off.
"""

from __future__ import annotations

import ast
import io
import sys
import traceback as traceback_mod
from collections.abc import Callable
from dataclasses import dataclass, field

CELL_FILENAME = "<cell>"

StreamSink = Callable[[str], None]


@dataclass
class CellOutcome:
    """Terminal state of one cell.  Produced exactly once per cell."""

    status: str  # "ok" | "error"
    result_text: str | None = None
    ename: str = ""
    evalue: str = ""
    traceback: list[str] = field(default_factory=list)

    @classmethod
    def ok(cls, result_text: str | None = None) -> "CellOutcome":
        return cls(status="ok", result_text=result_text)

    @classmethod
    def error(cls, exc: BaseException) -> "CellOutcome":
        return cls(
            status="error",
            ename=type(exc).__name__,
            evalue=str(exc),
            traceback=format_traceback(exc),
        )

    @classmethod
    def interrupted(cls) -> "CellOutcome":
        return cls(
            status="error",
            ename="KeyboardInterrupt",
            evalue="",
            traceback=["KeyboardInterrupt"],
        )


def format_traceback(exc: BaseException) -> list[str]:
    """Format ``exc`` the way frontends expect, minus kernel frames."""
    if isinstance(exc, SyntaxError):
        # The useful location info lives on the exception itself.
        return traceback_mod.format_exception_only(type(exc), exc)
    tb = exc.__traceback__
    trimmed = tb
    while trimmed is not None and trimmed.tb_frame.f_code.co_filename != CELL_FILENAME:
        trimmed = trimmed.tb_next
    if trimmed is None:
        trimmed = tb  # no user frame found; keep whatever we have
    return traceback_mod.format_exception(type(exc), exc, trimmed)


def safe_repr(value: object) -> str:
    try:
        return repr(value)
    except BaseException as exc:  # a broken __repr__ must not kill the kernel
        return f"<repr() raised {type(exc).__name__}: {exc}>"


class StreamProxy:
    """Text file object that publishes complete lines as they are written.

    ``print()`` issues several small ``write()`` calls; buffering until a
    newline keeps each published ``stream`` message a whole line while
    still being effectively real-time.  ``flush()`` (also called at the
    end of every cell) publishes any partial line.
    """

    encoding = "utf-8"
    errors = "replace"

    def __init__(self, emit: StreamSink) -> None:
        self._emit = emit
        self._pending = ""

    def write(self, text: object) -> int:
        if not isinstance(text, str):
            text = str(text)
        self._pending += text
        lines = self._pending.split("\n")
        self._pending = lines.pop()
        for line in lines:
            self._emit(line + "\n")
        return len(text)

    def writelines(self, lines) -> None:
        for line in lines:
            self.write(line)

    def flush(self) -> None:
        if self._pending:
            self._emit(self._pending)
            self._pending = ""

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False

    def isatty(self) -> bool:
        return False

    def fileno(self) -> int:
        raise io.UnsupportedOperation("fileno")


class _NoStdin:
    """``sys.stdin`` stand-in: interactive input is refused, never blocks."""

    closed = True

    def _refuse(self, *_args):
        raise EOFError("this kernel does not support stdin interaction")

    read = _refuse
    readline = _refuse
    readlines = _refuse

    def __iter__(self):
        return self

    def __next__(self):
        self._refuse()

    def isatty(self) -> bool:
        return False

    def readable(self) -> bool:
        return False

    def fileno(self) -> int:
        raise io.UnsupportedOperation("fileno")

    def flush(self) -> None:
        pass


class ExecutionContext:
    """Owns the shared user namespace and runs one cell at a time."""

    def __init__(self) -> None:
        self.namespace: dict = {"__name__": "__main__", "__doc__": None}

    def run_cell(
        self,
        code: str,
        emit_stdout: StreamSink,
        emit_stderr: StreamSink,
    ) -> CellOutcome:
        """Run one cell and report its terminal state.

        ``KeyboardInterrupt`` raised by the user code itself (or delivered
        by the interrupt controller) is converted into an "interrupted"
        outcome; every other ``BaseException`` becomes an error outcome.
        Nothing user code does can propagate out of here and wedge the
        kernel.
        """
        out_proxy = StreamProxy(emit_stdout)
        err_proxy = StreamProxy(emit_stderr)
        real_stdio = (sys.stdout, sys.stderr, sys.stdin)
        sys.stdout, sys.stderr, sys.stdin = out_proxy, err_proxy, _NoStdin()  # type: ignore[assignment]
        try:
            try:
                exec_code, eval_code = self._compile_cell(code)
                if exec_code is not None:
                    exec(exec_code, self.namespace)
                if eval_code is None:
                    return CellOutcome.ok()
                value = eval(eval_code, self.namespace)
            except KeyboardInterrupt:
                return CellOutcome.interrupted()
            except BaseException as exc:  # user code may raise anything
                return CellOutcome.error(exc)
            if value is None:
                return CellOutcome.ok()
            return CellOutcome.ok(result_text=safe_repr(value))
        finally:
            try:
                out_proxy.flush()
                err_proxy.flush()
            finally:
                sys.stdout, sys.stderr, sys.stdin = real_stdio

    @staticmethod
    def _compile_cell(code: str):
        """Split a cell into an ``exec`` part and an optional final expression."""
        tree = ast.parse(code, filename=CELL_FILENAME, mode="exec")
        exec_tree: ast.Module | None = tree
        eval_tree: ast.Expression | None = None
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            last = tree.body[-1]
            exec_body = tree.body[:-1]
            exec_tree = ast.Module(body=exec_body, type_ignores=[]) if exec_body else None
            eval_tree = ast.Expression(last.value)
            ast.fix_missing_locations(eval_tree)
        exec_code = compile(exec_tree, CELL_FILENAME, "exec") if exec_tree is not None else None
        eval_code = compile(eval_tree, CELL_FILENAME, "eval") if eval_tree is not None else None
        return exec_code, eval_code
