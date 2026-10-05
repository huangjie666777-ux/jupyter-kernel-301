"""内核主体: 通道线程、串行执行循环、消息分发与关闭。

线程模型(执行与通信分离):
  - 主线程    串行执行单元(exec_loop), 也是 SIGINT 的落点
  - shell     接收 shell 请求; execute_reply 也经它发送(套接字单线程持有)
  - control   中断 / 关闭, 与执行并行处理
  - heartbeat 心跳回显
  - iopub     各线程经带锁的 IOPublisher 发布
"""
from __future__ import annotations

import logging
import os
import platform
import queue
import threading
import time

import zmq

from . import __version__
from .connection import ConnectionInfo
from .execution import CellOutcome, Executor
from .heartbeat import Heartbeat
from .iopub import IOPublisher
from .security import BadMessage, new_session, recv_request

log = logging.getLogger("minikernel")

POLL_MS = 20  # 通道线程轮询间隔


def kernel_info_content() -> dict:
    """kernel_info_reply 内容(协议 5.3)。"""
    return {
        "status": "ok",
        "protocol_version": "5.3",
        "implementation": "minikernel",
        "implementation_version": __version__,
        "language_info": {
            "name": "python",
            "mimetype": "text/x-python",
            "version": platform.python_version(),
            "file_extension": ".py",
            "pygments_lexer": "ipython3",
            "codemirror_mode": {"name": "ipython", "version": 3},
            "nbconvert_exporter": "python",
        },
        "banner": f"minikernel {__version__}: 纯 Python Jupyter 协议 5.3 内核 (Python {platform.python_version()})",
        "help_links": [],
        "debugger": False,
    }


class _Request:
    """一个已认证的请求: 路由 idents + 完整消息(保留父消息关联)。"""

    __slots__ = ("idents", "msg")

    def __init__(self, idents, msg):
        self.idents = idents
        self.msg = msg


class Kernel:
    """Jupyter 协议 5.3 纯后端内核。"""

    def __init__(self, conn: ConnectionInfo) -> None:
        self.conn = conn
        self.executor = Executor()
        self.stopping = threading.Event()
        self._exec_queue: queue.Queue[_Request] = queue.Queue()
        self._shell_replies: queue.Queue = queue.Queue()
        self._cell_seq = 0

        self.ctx = zmq.Context()
        self.shell_sock = self._bind(zmq.ROUTER, conn.shell_port)
        self.control_sock = self._bind(zmq.ROUTER, conn.control_port)
        self.stdin_sock = self._bind(zmq.ROUTER, conn.stdin_port)  # 仅绑定, 永不发起 input_request
        self.hb_sock = self._bind(zmq.REP, conn.hb_port)
        self.iopub_sock = self._bind(zmq.PUB, conn.iopub_port)
        self.iopub = IOPublisher(new_session(conn.key, conn.signature_scheme), self.iopub_sock)

        self._threads = [
            threading.Thread(target=self._shell_loop, name="shell", daemon=True),
            threading.Thread(target=self._control_loop, name="control", daemon=True),
            Heartbeat(self.hb_sock, self.stopping),
        ]

    # ---- 生命周期 ----

    def _bind(self, kind: int, port: int) -> zmq.Socket:
        sock = self.ctx.socket(kind)
        sock.setsockopt(zmq.LINGER, 1000)  # 关闭时最多等 1s 把滞留消息发出去
        sock.bind(self.conn.endpoint(port))
        return sock

    def start(self) -> None:
        for t in self._threads:
            t.start()
        log.info(
            "已绑定 tcp://%s shell=%d control=%d iopub=%d stdin=%d hb=%d",
            self.conn.ip,
            self.conn.shell_port,
            self.conn.control_port,
            self.conn.iopub_port,
            self.conn.stdin_port,
            self.conn.hb_port,
        )

    def exec_loop(self) -> None:
        """主线程: 串行消费执行请求, 直到收到关闭。"""
        while not self.stopping.is_set():
            try:
                req = self._exec_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            except KeyboardInterrupt:
                continue  # 迟到的 SIGINT 落在单元间隙: 吞掉, 不影响随后单元
            try:
                self._run_one(req)
            except Exception:
                log.exception("处理执行请求时发生内部错误")

    def initiate_shutdown(self) -> None:
        """由通道线程触发: 通知主线程退出, 并兜底保证进程被回收。"""
        if self.stopping.is_set():
            return
        log.info("开始关闭内核")
        self.stopping.set()
        self.executor.interrupt()  # 打断正在运行的单元, 让主线程尽快收尾
        threading.Thread(target=self._watchdog, name="watchdog", daemon=True).start()

    def _watchdog(self) -> None:
        time.sleep(3.0)
        log.warning("优雅关闭超时, 强制退出进程")
        os._exit(0)

    def close(self) -> None:
        """回收全部线程与套接字(主线程在 exec_loop 退出后调用)。"""
        self.stopping.set()
        for t in self._threads:
            t.join(timeout=2.0)
        self.iopub.close()
        for sock in (self.shell_sock, self.control_sock, self.stdin_sock, self.hb_sock, self.iopub_sock):
            sock.close()
        self.ctx.term()
        log.info("线程与套接字已回收")

    # ---- shell 通道 ----

    def _shell_loop(self) -> None:
        session = new_session(self.conn.key, self.conn.signature_scheme)
        poller = zmq.Poller()
        poller.register(self.shell_sock, zmq.POLLIN)
        while not self.stopping.is_set():
            self._flush_shell_replies(session)
            if not poller.poll(POLL_MS):
                continue
            try:
                idents, msg = recv_request(session, self.shell_sock)
            except BadMessage as exc:
                log.warning("shell: 丢弃非法消息(%s)", exc)
                continue
            self._dispatch_shell(session, idents, msg)

    def _flush_shell_replies(self, session) -> None:
        """execute_reply 只能由 shell 线程在 shell 套接字上发送。"""
        while True:
            try:
                idents, parent, content = self._shell_replies.get_nowait()
            except queue.Empty:
                return
            try:
                session.send(self.shell_sock, "execute_reply", content, parent=parent, ident=idents)
            except zmq.ZMQError:
                log.warning("execute_reply 发送失败", exc_info=True)

    def _dispatch_shell(self, session, idents, msg) -> None:
        msg_type = msg["header"]["msg_type"]
        if msg_type == "kernel_info_request":
            self._reply_simple(session, self.shell_sock, idents, msg, "kernel_info_reply", kernel_info_content())
        elif msg_type == "execute_request":
            self._exec_queue.put(_Request(idents, msg))
        elif msg_type == "is_complete_request":
            self._reply_simple(session, self.shell_sock, idents, msg, "is_complete_reply", {"status": "complete"})
        elif msg_type == "comm_info_request":
            self._reply_simple(session, self.shell_sock, idents, msg, "comm_info_reply", {"status": "ok", "comms": {}})
        elif msg_type == "shutdown_request":
            self._reply_shutdown(session, self.shell_sock, idents, msg)
        else:
            log.warning("shell: 暂不支持的消息类型 %s", msg_type)

    def _reply_simple(self, session, sock, idents, msg, reply_type, content) -> None:
        self.iopub.status("busy", msg)
        session.send(sock, reply_type, content, parent=msg, ident=idents)
        self.iopub.status("idle", msg)

    def _reply_shutdown(self, session, sock, idents, msg) -> None:
        restart = bool(msg.get("content", {}).get("restart", False))
        self._reply_simple(session, sock, idents, msg, "shutdown_reply", {"status": "ok", "restart": restart})
        self.initiate_shutdown()

    # ---- control 通道 ----

    def _control_loop(self) -> None:
        session = new_session(self.conn.key, self.conn.signature_scheme)
        poller = zmq.Poller()
        poller.register(self.control_sock, zmq.POLLIN)
        while not self.stopping.is_set():
            if not poller.poll(POLL_MS):
                continue
            try:
                idents, msg = recv_request(session, self.control_sock)
            except BadMessage as exc:
                log.warning("control: 丢弃非法消息(%s)", exc)
                continue
            self._dispatch_control(session, idents, msg)

    def _dispatch_control(self, session, idents, msg) -> None:
        msg_type = msg["header"]["msg_type"]
        if msg_type == "interrupt_request":
            self.iopub.status("busy", msg)
            session.send(self.control_sock, "interrupt_reply", {"status": "ok"}, parent=msg, ident=idents)
            interrupted = self.executor.interrupt()
            self.iopub.status("idle", msg)
            log.info("control: 中断请求(%s)", "已投递" if interrupted else "无运行单元, 忽略")
        elif msg_type == "shutdown_request":
            self._reply_shutdown(session, self.control_sock, idents, msg)
        elif msg_type == "kernel_info_request":
            self._reply_simple(session, self.control_sock, idents, msg, "kernel_info_reply", kernel_info_content())
        else:
            log.warning("control: 暂不支持的消息类型 %s", msg_type)

    # ---- 执行(仅主线程) ----

    def _run_one(self, req: _Request) -> None:
        """处理一个 execute_request。

        终态(execute_reply + status idle)只由本线程发出, 每单元恰好一次;
        中断只改变终态的内容(ok/error), 不产生第二个终态。
        """
        parent = req.msg
        content = parent.get("content") or {}
        code = content.get("code", "")
        silent = bool(content.get("silent", False))
        # silent 强制 store_history 为 False(协议语义)
        store_history = bool(content.get("store_history", True)) and not silent

        self.iopub.status("busy", parent)
        finalized = False
        try:
            if store_history:
                self.executor.execution_count += 1
            count = self.executor.execution_count
            if not silent:
                self.iopub.execute_input(code, count, parent)

            self._cell_seq += 1
            emit_out = _drop if silent else lambda text: self.iopub.stream("stdout", text, parent)
            emit_err = _drop if silent else lambda text: self.iopub.stream("stderr", text, parent)
            outcome = self.executor.run_cell(self._cell_seq, code, emit_out, emit_err)

            if not silent:
                if outcome.status == "ok" and outcome.result_text is not None:
                    self.iopub.execute_result(count, outcome.result_text, parent)
                elif outcome.status == "error":
                    self.iopub.error(outcome.ename, outcome.evalue, outcome.traceback, parent)
            self._queue_execute_reply(req, outcome, count)
            finalized = True
        except KeyboardInterrupt:
            # 兜底: 迟到中断落在收尾阶段, 补齐该单元终态(不重复)
            if not finalized:
                outcome = CellOutcome(status="error", ename="KeyboardInterrupt", traceback=["KeyboardInterrupt"])
                self._queue_execute_reply(req, outcome, self.executor.execution_count)
        finally:
            self.iopub.status("idle", parent)

    def _queue_execute_reply(self, req: _Request, outcome: CellOutcome, count: int) -> None:
        content = {
            "status": outcome.status,
            "execution_count": count,
            "payload": [],
            "user_expressions": {},  # 不支持用户表达式
        }
        if outcome.status == "error":
            content.update(ename=outcome.ename, evalue=outcome.evalue, traceback=outcome.traceback)
        self._shell_replies.put((req.idents, req.msg, content))


def _drop(text: str) -> None:
    """silent 单元的输出直接丢弃。"""
