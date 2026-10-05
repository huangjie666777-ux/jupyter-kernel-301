"""Interrupt coordination between the control thread and the main thread.

User code runs in the *main* thread so that ordinary Python signal
handlers apply to it.  The control thread (which owns the control socket)
asks for an interrupt; this controller makes the delivery race-safe:

* every cell execution gets a monotonically increasing generation id;
* an interrupt request is tied to the generation that was running when
  the request arrived;
* a signal that is delivered late (its cell already finished) is
  swallowed, so a completed cell never gets a second terminal state;
* a signal can never match a *later* generation, so an interrupt cannot
  accidentally kill a subsequent cell.
"""

from __future__ import annotations

import logging
import signal
import threading

log = logging.getLogger(__name__)


class InterruptController:
    """Race-safe delivery of ``KeyboardInterrupt`` to the running cell."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._generation = 0
        self._active_id: int | None = None
        self._target_id: int | None = None
        # threading.get_ident() of the main thread, recorded before the
        # first cell runs; needed to aim SIGINT at the main thread.
        self.main_thread_id: int | None = None
        # Written from signal handlers.  A plain attribute assignment is
        # signal-safe in CPython; the main loop polls it between cells.
        self.external_stop_requested = False

    def install_main_thread(self) -> None:
        """Record the main thread id.  Called once from the main thread."""
        self.main_thread_id = threading.get_ident()

    # ------------------------------------------------------------------
    # main thread: mark the execution window
    # ------------------------------------------------------------------
    def begin(self) -> int:
        """Mark the start of a cell; return its generation id."""
        with self._lock:
            self._generation += 1
            self._active_id = self._generation
            if self._target_id is not None and self._target_id != self._active_id:
                # Stale request aimed at a cell that no longer exists.
                self._target_id = None
            return self._active_id

    def end(self, exec_id: int) -> None:
        """Mark the end of a cell.  Idempotent by design."""
        with self._lock:
            if self._active_id == exec_id:
                self._active_id = None
            if self._target_id == exec_id:
                self._target_id = None

    # ------------------------------------------------------------------
    # control thread: request an interrupt
    # ------------------------------------------------------------------
    def request_interrupt(self) -> bool:
        """Interrupt the currently running cell, if any.

        Returns True when a signal was raised (i.e. a cell was running).
        """
        with self._lock:
            if self._active_id is None:
                return False
            self._target_id = self._active_id
            main_id = self.main_thread_id
        # The signal must reach the *main* thread so that a blocked C
        # call there (e.g. time.sleep) is interrupted with EINTR and the
        # Python handler runs.  raise_signal() would only hit the calling
        # (control) thread.  get_ident() -- not get_native_id() -- is the
        # pthread_t that pthread_kill expects.
        if main_id is not None and hasattr(signal, "pthread_kill"):
            signal.pthread_kill(main_id, signal.SIGINT)
        else:  # pragma: no cover - non-pthread platforms
            signal.raise_signal(signal.SIGINT)
        return True

    # ------------------------------------------------------------------
    # main thread: signal handlers.  These must not take locks: the main
    # thread itself may hold ``self._lock`` when the signal arrives.
    # Attribute loads/stores are atomic under the GIL, and the only
    # cross-thread write that matters (``_target_id`` in
    # ``request_interrupt``) always happens-before the signal it raises.
    # ------------------------------------------------------------------
    def handle_sigint(self, signum, frame) -> None:  # noqa: ARG002
        target = self._target_id
        active = self._active_id
        if target is not None:
            self._target_id = None
            if active == target:
                raise KeyboardInterrupt
            return  # late signal for a finished cell: swallow it
        if active is not None:
            # External Ctrl+C while a cell runs: interrupt the cell.
            raise KeyboardInterrupt
        # External Ctrl+C while idle: ask the main loop to shut down.
        self.external_stop_requested = True

    def handle_sigterm(self, signum, frame) -> None:  # noqa: ARG002
        self.external_stop_requested = True
        if self._active_id is not None:
            # Unwind the running cell first; the main loop will see the
            # stop request right after the cell ends.
            raise KeyboardInterrupt


def block_process_signals() -> None:
    """Block SIGINT/SIGTERM in the calling (worker) thread.

    Worker threads block these signals so that process-directed signals
    are always delivered to the main thread, where the Python-level
    handlers (and user code) live.
    """
    if hasattr(signal, "pthread_sigmask"):
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
