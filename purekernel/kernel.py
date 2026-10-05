"""Kernel core: socket wiring, message dispatch, serial execution, shutdown.

Threading model (each ZeroMQ socket is owned by exactly one thread)::

    main thread      shell ROUTER  + stdin ROUTER (bound, never read)
                     runs the dispatch loop and executes every cell, so
                     Python signal handlers apply to user code
    control thread   control ROUTER (interrupt / shutdown / kernel_info)
    heartbeat thread heartbeat REP (echo; answers during long cells)
    iopub thread     IOPub PUB (fed through a FIFO queue, see iopub.py)

Authentication, framing and routing reuse
:class:`jupyter_client.session.Session`: messages with a bad HMAC
signature (or malformed frames) are dropped, and every reply carries the
request's routing identities and parent header back.
"""

from __future__ import annotations

import logging
import os
import platform
import signal
import threading

import zmq
from jupyter_client.session import Session

from .connection import ConnectionInfo
from .execution import CellOutcome, ExecutionContext
from .heartbeat import Heartbeat
from .interrupts import InterruptController, block_process_signals
from .iopub import IOPubPublisher

log = logging.getLogger(__name__)

KERNEL_IMPLEMENTATION = "purekernel"
KERNEL_IMPLEMENTATION_VERSION = "1.0.0"
PROTOCOL_VERSION = "5.3"

_SHUTDOWN_WATCHDOG_SECONDS = 3.0


class Kernel:
    """A pure-Python Jupyter kernel speaking protocol 5.3."""

    def __init__(self, conn: ConnectionInfo) -> None:
        self.conn = conn
        self.session = Session(key=conn.key, signature_scheme=conn.signature_scheme)
        self.execution_count = 0
        self.stop_event = threading.Event()
        self.interrupts = InterruptController()
        self.exec_ctx = ExecutionContext()
        self._shutdown_lock = threading.Lock()
        self._shutdown_initiated = False

        context = zmq.Context.instance()
        self.iopub = IOPubPublisher(self.session, conn.iopub_endpoint, context)
        self.heartbeat = Heartbeat(conn.hb_endpoint, self.stop_event, context)
        self._control_thread = threading.Thread(
            target=self._control_loop, name="purekernel-control", daemon=True
        )

    # ------------------------------------------------------------------
    # main loop (shell channel + cell execution)
    # ------------------------------------------------------------------
    def run(self) -> None:
        signal.signal(signal.SIGINT, self.interrupts.handle_sigint)
        signal.signal(signal.SIGTERM, self.interrupts.handle_sigterm)
        self.interrupts.install_main_thread()

        self.iopub.start()
        self.heartbeat.start()

        context = zmq.Context.instance()
        shell = context.socket(zmq.ROUTER)
        shell.linger = 0
        shell.bind(self.conn.shell_endpoint)
        # Bound so the port is owned by us; stdin interaction is refused
        # inside cells, so the socket is intentionally never read.
        stdin_sock = context.socket(zmq.ROUTER)
        stdin_sock.linger = 0
        stdin_sock.bind(self.conn.stdin_endpoint)

        self._control_thread.start()
        log.info(
            "purekernel ready (pid %d): shell=%s control=%s iopub=%s hb=%s",
            os.getpid(),
            self.conn.shell_endpoint,
            self.conn.control_endpoint,
            self.conn.iopub_endpoint,
            self.conn.hb_endpoint,
        )

        poller = zmq.Poller()
        poller.register(shell, zmq.POLLIN)
        try:
            while not self._stop_requested():
                try:
                    events = dict(poller.poll(100))
                except KeyboardInterrupt:
                    continue  # stray signal while idle; stop flags re-checked
                except zmq.ZMQError as exc:
                    if exc.errno == zmq.EINTR:
                        continue
                    raise
                if shell not in events:
                    continue
                try:
                    self._handle_shell(shell)
                except KeyboardInterrupt:
                    continue  # e.g. SIGTERM landed mid-dispatch; loop re-checks
                except Exception:
                    log.exception("unhandled error in shell dispatch")
        finally:
            self._shutdown()
            shell.close(0)
            stdin_sock.close(0)

    def _stop_requested(self) -> bool:
        return self.stop_event.is_set() or self.interrupts.external_stop_requested

    def _shutdown(self) -> None:
        """Reclaim sockets and threads; called once from the main loop."""
        log.info("shutting down")
        self.stop_event.set()
        self._control_thread.join(timeout=5)
        self.heartbeat.stop()
        self.iopub.stop()
        log.info("shutdown complete")

    # ------------------------------------------------------------------
    # message reception (shared by shell and control loops)
    # ------------------------------------------------------------------
    def _recv(self, sock) -> tuple[list, dict] | None:
        """Receive one authenticated message; drop anything invalid."""
        try:
            idents, msg = self.session.recv(sock, mode=0)
        except ValueError as exc:
            # Bad HMAC signature, missing <IDS|MSG> delimiter, broken JSON.
            log.warning("dropping unauthenticated/malformed message: %s", exc)
            return None
        except zmq.ZMQError:
            log.exception("zmq recv failed")
            return None
        if msg is None:
            return None
        return idents, msg

    # ------------------------------------------------------------------
    # shell channel
    # ------------------------------------------------------------------
    def _handle_shell(self, shell) -> None:
        received = self._recv(shell)
        if received is None:
            return
        idents, msg = received
        msg_type = msg.get("header", {}).get("msg_type", "")
        if msg_type == "kernel_info_request":
            self._handle_kernel_info(shell, idents, msg)
        elif msg_type == "execute_request":
            self._handle_execute(shell, idents, msg)
        elif msg_type == "shutdown_request":
            self._handle_shutdown(shell, idents, msg)
        else:
            log.warning("ignoring unsupported shell message %r", msg_type)

    def _handle_kernel_info(self, sock, idents, msg) -> None:
        self._publish_status("busy", msg)
        try:
            content = {
                "status": "ok",
                "protocol_version": PROTOCOL_VERSION,
                "implementation": KERNEL_IMPLEMENTATION,
                "implementation_version": KERNEL_IMPLEMENTATION_VERSION,
                "language_info": {
                    "name": "python",
                    "version": platform.python_version(),
                    "mimetype": "text/x-python",
                    "codemirror_mode": {"name": "ipython", "version": 3},
                    "pygments_lexer": "ipython3",
                    "nbconvert_exporter": "python",
                    "file_extension": ".py",
                },
                "banner": (
                    f"purekernel {KERNEL_IMPLEMENTATION_VERSION}: a pure-Python "
                    f"Jupyter kernel (protocol {PROTOCOL_VERSION})"
                ),
                "help_links": [],
                "debugger": False,
            }
            self.session.send(sock, "kernel_info_reply", content, parent=msg, ident=idents)
        finally:
            self._publish_status("idle", msg)

    def _handle_execute(self, sock, idents, msg) -> None:
        content = msg.get("content") or {}
        code = content.get("code") or ""
        if not isinstance(code, str):
            code = str(code)
        silent = bool(content.get("silent", False))
        # silent forces store_history off (protocol 5.3)
        store_history = bool(content.get("store_history", not silent)) and not silent
        if store_history:
            self.execution_count += 1
        count = self.execution_count

        self._publish_status("busy", msg)
        try:
            if not silent:
                self.iopub.publish(
                    "execute_input",
                    {"code": code, "execution_count": count},
                    parent=msg,
                )

            def make_emit(stream_name: str):
                def emit(text: str) -> None:
                    if not silent:
                        self.iopub.publish(
                            "stream", {"name": stream_name, "text": text}, parent=msg
                        )

                return emit

            outcome = self._run_cell(code, make_emit("stdout"), make_emit("stderr"))

            if outcome.status == "ok":
                if outcome.result_text is not None and not silent:
                    self.iopub.publish(
                        "execute_result",
                        {
                            "execution_count": count,
                            "data": {"text/plain": outcome.result_text},
                            "metadata": {},
                        },
                        parent=msg,
                    )
                # user_expressions are not supported: always empty.
                reply = {
                    "status": "ok",
                    "execution_count": count,
                    "payload": [],
                    "user_expressions": {},
                }
            else:
                error_fields = {
                    "ename": outcome.ename,
                    "evalue": outcome.evalue,
                    "traceback": outcome.traceback,
                }
                if not silent:
                    self.iopub.publish("error", error_fields, parent=msg)
                reply = {
                    "status": "error",
                    "execution_count": count,
                    **error_fields,
                }

            self.session.send(sock, "execute_reply", reply, parent=msg, ident=idents)
        except Exception:
            log.exception("execute handling failed; sending fallback error reply")
            try:
                self.session.send(
                    sock,
                    "execute_reply",
                    {
                        "status": "error",
                        "execution_count": count,
                        "ename": "InternalError",
                        "evalue": "kernel internal error",
                        "traceback": [],
                    },
                    parent=msg,
                    ident=idents,
                )
            except Exception:
                log.exception("failed to send fallback execute_reply")
        finally:
            self._publish_status("idle", msg)

    def _run_cell(self, code, emit_stdout, emit_stderr) -> CellOutcome:
        """Run one cell inside an interruptible window; resolve races.

        Exactly one :class:`CellOutcome` comes out of here, no matter how
        completion and interrupt requests interleave.
        """
        exec_id = self.interrupts.begin()
        outcome = None
        try:
            try:
                outcome = self.exec_ctx.run_cell(code, emit_stdout, emit_stderr)
            finally:
                self.interrupts.end(exec_id)
        except KeyboardInterrupt:
            # The interrupt signal landed in the teardown bookkeeping
            # instead of inside user code.  The cell is over either way;
            # make sure the window is closed and keep any outcome that
            # was already produced (completion wins the race).
            self.interrupts.end(exec_id)
        if outcome is None:
            outcome = CellOutcome.interrupted()
        return outcome

    # ------------------------------------------------------------------
    # control channel (own thread: works while a cell is executing)
    # ------------------------------------------------------------------
    def _control_loop(self) -> None:
        block_process_signals()
        context = zmq.Context.instance()
        sock = context.socket(zmq.ROUTER)
        sock.linger = 0
        try:
            sock.bind(self.conn.control_endpoint)
        except zmq.ZMQError:
            log.exception("failed to bind control channel")
            self.stop_event.set()
            sock.close(0)
            return
        log.debug("control bound on %s", self.conn.control_endpoint)
        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        try:
            while not self.stop_event.is_set():
                try:
                    events = dict(poller.poll(50))
                except zmq.ZMQError:
                    continue
                if sock not in events:
                    continue
                received = self._recv(sock)
                if received is None:
                    continue
                idents, msg = received
                try:
                    self._dispatch_control(sock, idents, msg)
                except Exception:
                    log.exception("unhandled error in control dispatch")
        finally:
            sock.close(0)

    def _dispatch_control(self, sock, idents, msg) -> None:
        msg_type = msg.get("header", {}).get("msg_type", "")
        if msg_type == "interrupt_request":
            self._publish_status("busy", msg)
            try:
                interrupted = self.interrupts.request_interrupt()
                log.info("interrupt requested (cell running: %s)", interrupted)
                self.session.send(
                    sock, "interrupt_reply", {"status": "ok"}, parent=msg, ident=idents
                )
            finally:
                self._publish_status("idle", msg)
        elif msg_type == "shutdown_request":
            self._handle_shutdown(sock, idents, msg)
        elif msg_type == "kernel_info_request":
            self._handle_kernel_info(sock, idents, msg)
        else:
            log.warning("ignoring unsupported control message %r", msg_type)

    def _handle_shutdown(self, sock, idents, msg) -> None:
        restart = bool((msg.get("content") or {}).get("restart", False))
        self._publish_status("busy", msg)
        try:
            self.session.send(
                sock,
                "shutdown_reply",
                {"status": "ok", "restart": restart},
                parent=msg,
                ident=idents,
            )
        finally:
            self._publish_status("idle", msg)
        log.info("shutdown requested (restart=%s)", restart)
        self.initiate_shutdown()

    # ------------------------------------------------------------------
    # shutdown
    # ------------------------------------------------------------------
    def initiate_shutdown(self) -> None:
        """Begin a clean shutdown; safe to call from any thread."""
        with self._shutdown_lock:
            if self._shutdown_initiated:
                return
            self._shutdown_initiated = True
        self.stop_event.set()
        # Unblock a running cell so the main loop can exit promptly.
        self.interrupts.request_interrupt()
        # If user code swallows the interrupt and never returns, do not
        # let it hold the process (and its ports) hostage.
        watchdog = threading.Timer(_SHUTDOWN_WATCHDOG_SECONDS, self._force_exit)
        watchdog.daemon = True
        watchdog.start()

    @staticmethod
    def _force_exit() -> None:
        log.warning("clean shutdown timed out; forcing process exit")
        os._exit(0)

    # ------------------------------------------------------------------
    # IOPub helpers
    # ------------------------------------------------------------------
    def _publish_status(self, state: str, parent: dict) -> None:
        self.iopub.publish("status", {"execution_state": state}, parent=parent)
