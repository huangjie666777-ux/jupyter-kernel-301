"""执行上下文: 共享命名空间、实时输出转发、基于 SIGINT 的可中断执行。"""
from __future__ import annotations

import ast
import io
import os
import signal
import sys
import threading
import traceback
from dataclasses import dataclass, field


@dataclass
class CellOutcome:
    """单元的终态结果。每个单元恰好产生一个终态。"""

    status: str  # "ok" | "error"
    result_text: str | None = None  # 末尾表达式的 text/plain
    ename: str = ""
    evalue: str = ""
    traceback: list = field(default_factory=list)

    @classmethod
    def ok(cls, result_text: str | None = None) -> "CellOutcome":
        return cls(status="ok", result_text=result_text)

    @classmethod
    def error(cls, exc: BaseException) -> "CellOutcome":
        return cls(
            status="error",
            ename=type(exc).__name__,
            evalue=str(exc),
            traceback=traceback.format_exception(exc),
        )


class _StreamProxy(io.TextIOBase):
    """把单元内的 stdout/stderr 按行实时转发到 IOPub。"""

    def __init__(self, emit) -> None:
        self._emit = emit
        self._buf = ""

    def writable(self) -> bool:
        return True

    def write(self, text) -> int:
        text = str(text)
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            self._emit(line + "\n")
        return len(text)

    def flush(self) -> None:
        if self._buf:
            self._emit(self._buf)
            self._buf = ""


class _NoStdin(io.IOBase):
    """内核不支持 stdin 交互: 读操作立即报错, 而不是挂起等待。"""

    def readable(self) -> bool:
        return False

    def read(self, *args):
        raise RuntimeError("minikernel 不支持 stdin 交互输入")

    def readline(self, *args):
        raise RuntimeError("minikernel 不支持 stdin 交互输入")


def _compile_cell(code: str):
    """把单元编译成 (exec 部分, 末尾表达式 eval 部分)。

    末尾若是表达式语句, 单独以 eval 编译, 其值的 repr 作为 text/plain 结果。
    """
    tree = ast.parse(code, mode="exec")
    if tree.body and isinstance(tree.body[-1], ast.Expr):
        last = tree.body.pop()
        expr = ast.fix_missing_locations(ast.Expression(last.value))
        return compile(tree, "<cell>", "exec"), compile(expr, "<cell>", "eval")
    return compile(tree, "<cell>", "exec"), None


class Executor:
    """单元执行器。

    执行发生在主线程(串行消费队列); control 线程通过 interrupt()
    借助 SIGINT 把 KeyboardInterrupt 投递进当前单元。SIGINT 处理器
    只在中断确实瞄准当前运行单元时才抛出, 迟到的信号直接吞掉,
    不会误伤随后的单元。命名空间跨单元共享, 异常后依然存活。
    """

    def __init__(self) -> None:
        self.user_ns: dict = {"__name__": "__main__", "__doc__": None}
        self.execution_count = 0
        self._lock = threading.Lock()
        self._running_cell: int | None = None
        self._interrupt_target: int | None = None

    @property
    def current_cell(self) -> int | None:
        return self._running_cell

    # ---- 中断: control 线程 / 信号处理器调用 ----

    def interrupt(self) -> bool:
        """武装并投递中断; 没有单元在运行则为空操作。"""
        with self._lock:
            cell = self._running_cell
            if cell is None:
                return False
            self._interrupt_target = cell
        os.kill(os.getpid(), signal.SIGINT)
        return True

    def interrupt_from_signal_handler(self) -> None:
        """信号处理器内的无锁版本(如 SIGTERM 借道打断当前单元)。"""
        cell = self._running_cell
        if cell is not None:
            self._interrupt_target = cell
            os.kill(os.getpid(), signal.SIGINT)

    def handle_sigint(self, signum, frame) -> None:
        """SIGINT 处理器(主线程): 只响应瞄准当前单元的中断。"""
        if self._interrupt_target is not None and self._interrupt_target == self._running_cell:
            raise KeyboardInterrupt
        # 否则是迟到或外部的 SIGINT: 吞掉, 不影响随后的单元。

    # ---- 执行: 仅主线程 ----

    def run_cell(self, cell_id: int, code: str, emit_out, emit_err) -> CellOutcome:
        """执行一个单元并返回终态。实时输出经 emit_* 回调转发。"""
        out_proxy = _StreamProxy(emit_out)
        err_proxy = _StreamProxy(emit_err)
        old_stdio = (sys.stdout, sys.stderr, sys.stdin)
        with self._lock:
            self._running_cell = cell_id
        sys.stdout, sys.stderr, sys.stdin = out_proxy, err_proxy, _NoStdin()
        try:
            return CellOutcome.ok(self._exec(code))
        except BaseException as exc:  # 含 KeyboardInterrupt/SystemExit: 内核必须存活
            return CellOutcome.error(exc)
        finally:
            for proxy in (out_proxy, err_proxy):
                try:
                    proxy.flush()
                except Exception:
                    pass
            sys.stdout, sys.stderr, sys.stdin = old_stdio
            with self._lock:
                self._running_cell = None
                if self._interrupt_target == cell_id:
                    self._interrupt_target = None

    def _exec(self, code: str) -> str | None:
        exec_code, eval_code = _compile_cell(code)
        exec(exec_code, self.user_ns)
        if eval_code is None:
            return None
        value = eval(eval_code, self.user_ns)
        if value is None:
            return None
        self.user_ns["_"] = value
        return repr(value)
