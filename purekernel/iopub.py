"""IOPub publishing.

ZeroMQ sockets are not thread-safe, so the PUB socket is created, bound
and used exclusively by a dedicated thread.  Any other thread (the main
thread executing a cell, or the control thread) publishes by putting a
message on a FIFO queue; the owner thread forwards it.  A single queue
preserves the protocol ordering ``busy -> ... -> idle``.
"""

from __future__ import annotations

import logging
import queue
import threading

import zmq

from .interrupts import block_process_signals

log = logging.getLogger(__name__)


class IOPubPublisher:
    """Thread-safe publisher for the IOPub channel."""

    def __init__(self, session, endpoint: str, context: zmq.Context | None = None) -> None:
        self._session = session
        self._endpoint = endpoint
        self._context = context or zmq.Context.instance()
        self._queue: queue.Queue = queue.Queue()
        self._ready = threading.Event()
        self._error: Exception | None = None
        self._thread = threading.Thread(
            target=self._run, name="purekernel-iopub", daemon=True
        )

    def start(self) -> None:
        self._thread.start()
        if not self._ready.wait(timeout=10):
            raise RuntimeError(f"IOPub thread did not start on {self._endpoint}")
        if self._error is not None:
            raise RuntimeError(f"IOPub failed to bind {self._endpoint}: {self._error}")

    def publish(self, msg_type: str, content: dict, parent: dict | None = None) -> None:
        """Enqueue a message for the IOPub channel (any thread may call)."""
        self._queue.put((msg_type, content, parent))

    def stop(self) -> None:
        self._queue.put(None)
        self._thread.join(timeout=5)

    def _run(self) -> None:
        block_process_signals()
        socket = self._context.socket(zmq.PUB)
        socket.linger = 0
        try:
            socket.bind(self._endpoint)
        except zmq.ZMQError as exc:
            self._error = exc
            self._ready.set()
            socket.close(0)
            return
        self._ready.set()
        log.debug("iopub bound on %s", self._endpoint)
        try:
            while True:
                item = self._queue.get()
                if item is None:
                    break
                msg_type, content, parent = item
                try:
                    # The message type doubles as the SUB topic frame.
                    self._session.send(
                        socket,
                        msg_type,
                        content=content,
                        parent=parent,
                        ident=msg_type.encode("utf-8"),
                    )
                except Exception:
                    log.exception("failed to publish %r", msg_type)
        finally:
            socket.close(0)
