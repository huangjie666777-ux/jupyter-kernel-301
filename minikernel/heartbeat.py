"""心跳通道: REP 回显, 与执行完全解耦。"""
from __future__ import annotations

import threading

import zmq


class Heartbeat(threading.Thread):
    """原样回显收到的每一帧。

    独立线程 + 独立套接字: 主线程长时间执行 Python 代码时,
    心跳照常应答, 前端据此判断内核存活。
    """

    def __init__(self, socket: zmq.Socket, stopping: threading.Event) -> None:
        super().__init__(name="heartbeat", daemon=True)
        self._socket = socket
        self._stopping = stopping

    def run(self) -> None:
        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)
        while not self._stopping.is_set():
            if not poller.poll(100):
                continue
            try:
                frames = self._socket.recv_multipart()
                self._socket.send_multipart(frames)
            except zmq.ZMQError:
                break  # 套接字已关闭
