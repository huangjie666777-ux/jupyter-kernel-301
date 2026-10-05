"""Heartbeat channel.

A REP socket that echoes every ping back, served from its own thread so
it keeps answering while the main thread is busy executing a long cell.
"""

from __future__ import annotations

import logging
import threading

import zmq

from .interrupts import block_process_signals

log = logging.getLogger(__name__)


class Heartbeat:
    """Ping-echo server for the heartbeat channel."""

    def __init__(
        self,
        endpoint: str,
        stop_event: threading.Event,
        context: zmq.Context | None = None,
    ) -> None:
        self._endpoint = endpoint
        self._stop = stop_event
        self._context = context or zmq.Context.instance()
        self._ready = threading.Event()
        self._error: Exception | None = None
        self._thread = threading.Thread(
            target=self._run, name="purekernel-hb", daemon=True
        )

    def start(self) -> None:
        self._thread.start()
        if not self._ready.wait(timeout=10):
            raise RuntimeError(f"heartbeat thread did not start on {self._endpoint}")
        if self._error is not None:
            raise RuntimeError(f"heartbeat failed to bind {self._endpoint}: {self._error}")

    def stop(self) -> None:
        self._thread.join(timeout=5)

    def _run(self) -> None:
        block_process_signals()
        socket = self._context.socket(zmq.REP)
        socket.linger = 0
        try:
            socket.bind(self._endpoint)
        except zmq.ZMQError as exc:
            self._error = exc
            self._ready.set()
            socket.close(0)
            return
        self._ready.set()
        log.debug("heartbeat bound on %s", self._endpoint)
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        try:
            while not self._stop.is_set():
                try:
                    events = dict(poller.poll(100))
                except zmq.ZMQError:
                    continue
                if socket not in events:
                    continue
                try:
                    ping = socket.recv()
                    socket.send(ping)
                except zmq.ZMQError:
                    log.exception("heartbeat echo failed")
        finally:
            socket.close(0)
