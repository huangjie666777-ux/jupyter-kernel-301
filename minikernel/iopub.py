"""IOPub 发布: 状态、输入回显、流式输出、结果与异常。"""
from __future__ import annotations

import threading

import zmq


class IOPublisher:
    """PUB 套接字不是线程安全的, 这里用锁串行化所有发布。

    主线程(执行)与 shell/control 通道线程都通过它发消息。
    """

    def __init__(self, session, socket: zmq.Socket) -> None:
        self._session = session
        self._socket = socket
        self._lock = threading.Lock()
        self._closed = False

    def close(self) -> None:
        with self._lock:
            self._closed = True

    def publish(self, msg_type: str, content: dict, parent=None) -> None:
        topic = f"kernel.{self._session.session}.{msg_type}".encode()
        with self._lock:
            if self._closed:
                return
            try:
                self._session.send(self._socket, msg_type, content, parent=parent, ident=topic)
            except zmq.ZMQError:
                pass  # 关闭竞态: 丢弃

    # ---- 协议消息 ----

    def status(self, state: str, parent) -> None:
        self.publish("status", {"execution_state": state}, parent)

    def execute_input(self, code: str, execution_count: int, parent) -> None:
        self.publish("execute_input", {"code": code, "execution_count": execution_count}, parent)

    def stream(self, name: str, text: str, parent) -> None:
        self.publish("stream", {"name": name, "text": text}, parent)

    def execute_result(self, execution_count: int, text: str, parent) -> None:
        self.publish(
            "execute_result",
            {
                "execution_count": execution_count,
                "data": {"text/plain": text},
                "metadata": {},
            },
            parent,
        )

    def error(self, ename: str, evalue: str, tb: list, parent) -> None:
        self.publish("error", {"ename": ename, "evalue": evalue, "traceback": tb}, parent)
