"""Integration tests for purekernel.

Every test boots a real kernel subprocess on a throwaway connection file
and talks to it with :class:`jupyter_client.BlockingKernelClient` -- the
same client library Jupyter frontends use.
"""

from __future__ import annotations

import json
import os
import queue
import socket
import subprocess
import sys
import time
import uuid

import pytest
import zmq
from jupyter_client import BlockingKernelClient
from jupyter_client.session import Session

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STARTUP_TIMEOUT = 20
CELL_TIMEOUT = 15


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def write_connection_file(dirpath: str):
    cfg = {
        "transport": "tcp",
        "ip": "127.0.0.1",
        "signature_scheme": "hmac-sha256",
        "key": uuid.uuid4().hex,
        "shell_port": free_port(),
        "control_port": free_port(),
        "iopub_port": free_port(),
        "stdin_port": free_port(),
        "hb_port": free_port(),
        "kernel_name": "purekernel",
    }
    path = os.path.join(dirpath, "connection.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(cfg, handle)
    return path, cfg


def make_client(conn_path: str) -> BlockingKernelClient:
    client = BlockingKernelClient()
    client.connection_file = conn_path
    client.load_connection_file()
    client.start_channels()
    return client


class RunningKernel:
    def __init__(self, proc, client, conn_path, cfg):
        self.proc = proc
        self.client = client
        self.conn_path = conn_path
        self.cfg = cfg


@pytest.fixture
def kernel(tmp_path):
    conn_path, cfg = write_connection_file(str(tmp_path))
    env = dict(os.environ)
    env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [sys.executable, "-m", "purekernel", "-f", conn_path],
        cwd=REPO_ROOT,
        env=env,
    )
    client = make_client(conn_path)
    try:
        client.wait_for_ready(timeout=STARTUP_TIMEOUT)
        yield RunningKernel(proc, client, conn_path, cfg)
    finally:
        try:
            client.shutdown()
        except Exception:
            pass
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        client.stop_channels()


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("timed out waiting for a kernel message")
    return remaining


def collect_for(client, msg_id, timeout=CELL_TIMEOUT):
    """Collect (execute_reply, [iopub messages]) for one execute_request."""
    deadline = time.monotonic() + timeout
    iopub = []
    while True:
        msg = client.get_iopub_msg(timeout=_remaining(deadline))
        if msg["parent_header"].get("msg_id") != msg_id:
            continue
        iopub.append(msg)
        if msg["msg_type"] == "status" and msg["content"]["execution_state"] == "idle":
            break
    while True:
        reply = client.get_shell_msg(timeout=_remaining(deadline))
        if reply["parent_header"].get("msg_id") == msg_id:
            return reply, iopub


def collect_cell(client, code, timeout=CELL_TIMEOUT, **execute_kwargs):
    msg_id = client.execute(code, **execute_kwargs)
    return collect_for(client, msg_id, timeout)


def wait_shell_reply(client, msg_id, timeout=CELL_TIMEOUT):
    deadline = time.monotonic() + timeout
    while True:
        reply = client.get_shell_msg(timeout=_remaining(deadline))
        if reply["parent_header"].get("msg_id") == msg_id:
            return reply


def wait_iopub(client, msg_id, msg_type, timeout=CELL_TIMEOUT):
    deadline = time.monotonic() + timeout
    while True:
        msg = client.get_iopub_msg(timeout=_remaining(deadline))
        if msg["parent_header"].get("msg_id") != msg_id:
            continue
        if msg["msg_type"] == msg_type:
            return msg


def send_interrupt(client, timeout=CELL_TIMEOUT):
    request = client.session.msg("interrupt_request", {})
    client.control_channel.send(request)
    deadline = time.monotonic() + timeout
    while True:
        reply = client.get_control_msg(timeout=_remaining(deadline))
        if reply["parent_header"].get("msg_id") == request["header"]["msg_id"]:
            assert reply["msg_type"] == "interrupt_reply"
            assert reply["content"]["status"] == "ok"
            return reply


def iopub_types(msgs):
    return [m["msg_type"] for m in msgs]


def stream_text(msgs, name):
    return "".join(
        m["content"]["text"]
        for m in msgs
        if m["msg_type"] == "stream" and m["content"]["name"] == name
    )


def results(msgs):
    return [m for m in msgs if m["msg_type"] == "execute_result"]


def errors(msgs):
    return [m for m in msgs if m["msg_type"] == "error"]


# ----------------------------------------------------------------------
# 1. startup / kernel_info
# ----------------------------------------------------------------------
def test_startup_and_kernel_info(kernel):
    assert kernel.proc.poll() is None
    reply = kernel.client.kernel_info(reply=True, timeout=CELL_TIMEOUT)
    content = reply["content"]
    assert content["status"] == "ok"
    assert content["protocol_version"] == "5.3"
    assert content["implementation"] == "purekernel"
    assert content["language_info"]["name"] == "python"


def test_kernel_info_on_control_channel(kernel):
    client = kernel.client
    request = client.session.msg("kernel_info_request", {})
    client.control_channel.send(request)
    deadline = time.monotonic() + CELL_TIMEOUT
    while True:
        reply = client.get_control_msg(timeout=_remaining(deadline))
        if reply["parent_header"].get("msg_id") == request["header"]["msg_id"]:
            break
    assert reply["msg_type"] == "kernel_info_reply"
    assert reply["content"]["implementation"] == "purekernel"


# ----------------------------------------------------------------------
# 2. execution, results, streams, shared namespace
# ----------------------------------------------------------------------
def test_execute_result_streams_and_count(kernel):
    reply, iopub = collect_cell(kernel.client, "x = 6 * 7\nprint('value is', x)\nx")
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["execution_count"] == 1

    types = iopub_types(iopub)
    assert types[0] == "status"
    assert iopub[0]["content"]["execution_state"] == "busy"
    assert "execute_input" in types
    assert stream_text(iopub, "stdout") == "value is 42\n"
    (result,) = results(iopub)
    assert result["content"]["data"] == {"text/plain": "42"}
    assert result["content"]["execution_count"] == 1
    assert iopub[-1]["msg_type"] == "status"
    assert iopub[-1]["content"]["execution_state"] == "idle"

    reply2, iopub2 = collect_cell(
        kernel.client, "import sys\nprint('warn', file=sys.stderr)"
    )
    assert reply2["content"]["execution_count"] == 2
    assert stream_text(iopub2, "stderr") == "warn\n"


def test_shared_namespace_across_cells(kernel):
    collect_cell(kernel.client, "total = 40")
    reply, iopub = collect_cell(kernel.client, "total += 2\ntotal")
    assert reply["content"]["status"] == "ok"
    assert results(iopub)[0]["content"]["data"]["text/plain"] == "42"


# ----------------------------------------------------------------------
# 3. exceptions and recovery
# ----------------------------------------------------------------------
def test_exception_reporting_and_recovery(kernel):
    collect_cell(kernel.client, "keep = 'alive'")
    reply, iopub = collect_cell(kernel.client, "1 / 0")
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "ZeroDivisionError"
    assert "division by zero" in reply["content"]["evalue"]
    assert reply["content"]["traceback"]
    (error_msg,) = errors(iopub)
    assert error_msg["content"]["ename"] == "ZeroDivisionError"
    assert error_msg["content"]["traceback"]

    # the kernel keeps executing afterwards, namespace intact
    reply, iopub = collect_cell(kernel.client, "keep")
    assert reply["content"]["status"] == "ok"
    assert results(iopub)[0]["content"]["data"]["text/plain"] == "'alive'"


def test_syntax_error_is_an_error_not_a_crash(kernel):
    reply, iopub = collect_cell(kernel.client, "def broken(:\n")
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "SyntaxError"
    reply, iopub = collect_cell(kernel.client, "'still here'")
    assert reply["content"]["status"] == "ok"


# ----------------------------------------------------------------------
# 4. silent / store_history semantics
# ----------------------------------------------------------------------
def test_silent_and_store_history_semantics(kernel):
    reply, _ = collect_cell(kernel.client, "1 + 1")
    assert reply["content"]["execution_count"] == 1

    # store_history=False: output is published, the counter does not move
    reply, iopub = collect_cell(kernel.client, "'no-history'", store_history=False)
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["execution_count"] == 1
    assert results(iopub)[0]["content"]["data"]["text/plain"] == "'no-history'"

    # silent=True: no cell output on IOPub, counter frozen, reply still sent
    reply, iopub = collect_cell(
        kernel.client, "print('hidden')\n'silent-result'", silent=True
    )
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["execution_count"] == 1
    assert iopub_types(iopub) == ["status", "status"]  # busy + idle only

    # the counter resumes from where it was
    reply, iopub = collect_cell(kernel.client, "'counted'")
    assert reply["content"]["execution_count"] == 2
    assert results(iopub)[0]["content"]["execution_count"] == 2


# ----------------------------------------------------------------------
# 5. stdin is refused, never hangs
# ----------------------------------------------------------------------
def test_stdin_input_is_refused_not_hung(kernel):
    reply, iopub = collect_cell(kernel.client, "input('are you there? ')")
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "EOFError"


# ----------------------------------------------------------------------
# 6. interrupt
# ----------------------------------------------------------------------
def test_interrupt_long_cell_preserves_namespace(kernel):
    client = kernel.client
    msg_id = client.execute(
        "import time\n"
        "marker = 'set-before-sleep'\n"
        "print('cell started', flush=True)\n"
        "time.sleep(60)\n"
        "marker = 'reached-the-end'\n"
    )
    # wait until the cell is really running before interrupting it
    wait_iopub(client, msg_id, "stream")
    send_interrupt(client)

    reply, iopub = collect_for(client, msg_id)
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "KeyboardInterrupt"
    assert errors(iopub)[0]["content"]["ename"] == "KeyboardInterrupt"

    # exactly one terminal state: no second execute_reply for this cell
    with pytest.raises(queue.Empty):
        client.get_shell_msg(timeout=1.0)

    # variables assigned before the interrupt survive; the tail never ran
    reply, iopub = collect_cell(client, "print('marker =', marker)")
    assert stream_text(iopub, "stdout") == "marker = set-before-sleep\n"

    # and subsequent cells execute normally
    reply, iopub = collect_cell(client, "6 * 7")
    assert reply["content"]["status"] == "ok"
    assert results(iopub)[0]["content"]["data"]["text/plain"] == "42"


def test_interrupt_while_idle_is_a_noop(kernel):
    send_interrupt(kernel.client)
    reply, iopub = collect_cell(kernel.client, "'unharmed'")
    assert reply["content"]["status"] == "ok"
    assert results(iopub)[0]["content"]["data"]["text/plain"] == "'unharmed'"


# ----------------------------------------------------------------------
# 7. heartbeat during long execution
# ----------------------------------------------------------------------
def test_heartbeat_answers_while_cell_runs(kernel):
    client = kernel.client
    msg_id = client.execute("import time\ntime.sleep(30)")
    wait_iopub(client, msg_id, "execute_input")

    req = zmq.Context.instance().socket(zmq.REQ)
    req.linger = 0
    req.connect(f"tcp://127.0.0.1:{kernel.cfg['hb_port']}")
    try:
        req.send(b"ping")
        assert req.poll(5000), "heartbeat did not answer while a cell was running"
        assert req.recv() == b"ping"
    finally:
        req.close(0)

    # leave the kernel clean for the fixture teardown
    send_interrupt(client)
    reply, _ = collect_for(client, msg_id)
    assert reply["content"]["ename"] == "KeyboardInterrupt"


# ----------------------------------------------------------------------
# 8. authentication
# ----------------------------------------------------------------------
def test_bad_signature_is_rejected(kernel):
    endpoint = f"tcp://127.0.0.1:{kernel.cfg['shell_port']}"
    evil = zmq.Context.instance().socket(zmq.DEALER)
    evil.linger = 0
    evil.connect(endpoint)
    try:
        forged = Session(key=b"forged-key", signature_scheme="hmac-sha256")
        forged.send(evil, "kernel_info_request", {})
        evil.send_multipart([b"complete", b"garbage"])
        assert evil.poll(1500) == 0, "kernel answered a forged message"
    finally:
        evil.close(0)

    # the kernel is unharmed and still answers honest clients
    reply = kernel.client.kernel_info(reply=True, timeout=CELL_TIMEOUT)
    assert reply["content"]["status"] == "ok"


# ----------------------------------------------------------------------
# 9. multi-client routing
# ----------------------------------------------------------------------
def test_replies_are_routed_to_the_right_client(kernel):
    client_a = kernel.client
    client_b = make_client(kernel.conn_path)
    try:
        client_b.wait_for_ready(timeout=STARTUP_TIMEOUT)
        msg_a = client_a.execute("import time\ntime.sleep(0.5)\n'A'")
        msg_b = client_b.execute("'B'")
        reply_a = wait_shell_reply(client_a, msg_a)
        reply_b = wait_shell_reply(client_b, msg_b)
        # each client got exactly its own reply (cells run serially)
        assert reply_a["content"]["status"] == "ok"
        assert reply_b["content"]["status"] == "ok"
        assert reply_a["content"]["execution_count"] == 1
        assert reply_b["content"]["execution_count"] == 2
    finally:
        client_b.stop_channels()


# ----------------------------------------------------------------------
# 10. shutdown
# ----------------------------------------------------------------------
def test_shutdown_request_stops_process_and_releases_ports(kernel):
    reply = kernel.client.shutdown(reply=True, timeout=CELL_TIMEOUT)
    assert reply["content"]["status"] == "ok"
    kernel.proc.wait(timeout=10)
    assert kernel.proc.returncode == 0

    # sockets are released: the shell port can be bound again right away
    sock = socket.socket()
    try:
        # SO_REUSEADDR mirrors zmq's own listeners and tolerates the
        # TIME_WAIT entries left by client connections; an actively bound
        # port would still fail this bind.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", kernel.cfg["shell_port"]))
    finally:
        sock.close()
