#!/usr/bin/env python3
"""Drive purekernel with a real Jupyter client.

Boots a fresh kernel process on a throwaway connection file and
demonstrates, end to end:

    1. startup + kernel_info handshake
    2. cell execution: stdout streaming and a text/plain result
    3. an exception (ename/evalue/traceback) and recovery afterwards
    4. interrupting a long-running cell (variables survive)
    5. heartbeat answering while a cell is running
    6. shutdown: the process exits and releases its ports

Run from the repository root:

    .venv/bin/python demo.py
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))


def free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def write_connection_file(dirpath: str) -> str:
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
        json.dump(cfg, handle, indent=2)
    return path


def section(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def show_iopub(msg) -> None:
    msg_type = msg["msg_type"]
    content = msg["content"]
    if msg_type == "status":
        print(f"  [iopub] status          -> {content['execution_state']}")
    elif msg_type == "execute_input":
        print(f"  [iopub] execute_input   -> count={content['execution_count']}")
    elif msg_type == "stream":
        print(f"  [iopub] stream/{content['name']:<6} -> {content['text']!r}")
    elif msg_type == "execute_result":
        print(f"  [iopub] execute_result  -> {content['data']}")
    elif msg_type == "error":
        print(f"  [iopub] error           -> {content['ename']}: {content['evalue']}")


def run_cell(client, code: str, **kwargs):
    """Execute one cell, printing IOPub traffic and the shell reply."""
    print(f"\n>>> {code}")
    msg_id = client.execute(code, **kwargs)
    while True:
        msg = client.get_iopub_msg(timeout=30)
        if msg["parent_header"].get("msg_id") != msg_id:
            continue
        show_iopub(msg)
        if msg["msg_type"] == "status" and msg["content"]["execution_state"] == "idle":
            break
    while True:
        reply = client.get_shell_msg(timeout=30)
        if reply["parent_header"].get("msg_id") == msg_id:
            break
    content = reply["content"]
    extra = f" ({content['ename']}: {content['evalue']})" if content["status"] == "error" else ""
    print(
        f"  [shell] execute_reply   -> status={content['status']}"
        f" count={content['execution_count']}{extra}"
    )
    return reply


def main() -> int:
    from jupyter_client import BlockingKernelClient

    tmp = tempfile.mkdtemp(prefix="purekernel-demo-")
    conn_path = write_connection_file(tmp)
    print(f"connection file: {conn_path}")

    proc = subprocess.Popen(
        [sys.executable, "-m", "purekernel", "-f", conn_path], cwd=REPO_ROOT
    )
    client = BlockingKernelClient()
    client.connection_file = conn_path
    client.load_connection_file()
    client.start_channels()
    try:
        section("1. startup: wait_for_ready + kernel_info")
        client.wait_for_ready(timeout=20)
        info = client.kernel_info(reply=True, timeout=10)["content"]
        print(f"  banner:  {info['banner']}")
        print(f"  python:  {info['language_info']['version']}")
        print(f"  wire:    protocol {info['protocol_version']}")

        section("2. execute: streaming stdout + text/plain result")
        run_cell(client, "x = 6 * 7\nprint('computing...')\nx")

        section("3. exception: error reply + traceback, kernel keeps going")
        run_cell(client, "1 / 0")
        run_cell(client, "print('x survived the exception:', x)")

        section("4. interrupt a 60s cell; variables set so far survive")
        msg_id = client.execute(
            "import time\n"
            "marker = 'set-before-sleep'\n"
            "print('sleeping 60s ...', flush=True)\n"
            "time.sleep(60)\n"
            "marker = 'reached-the-end'\n"
        )
        while True:  # wait until the cell is really running
            msg = client.get_iopub_msg(timeout=30)
            if msg["parent_header"].get("msg_id") != msg_id:
                continue
            show_iopub(msg)
            if msg["msg_type"] == "stream":
                break
        print("\n  >>> sending interrupt_request on the control channel")
        request = client.session.msg("interrupt_request", {})
        client.control_channel.send(request)
        while True:  # finish collecting the interrupted cell
            msg = client.get_iopub_msg(timeout=30)
            if msg["parent_header"].get("msg_id") != msg_id:
                continue
            show_iopub(msg)
            if msg["msg_type"] == "status" and msg["content"]["execution_state"] == "idle":
                break
        while True:
            reply = client.get_shell_msg(timeout=30)
            if reply["parent_header"].get("msg_id") == msg_id:
                break
        print(
            f"  [shell] execute_reply   -> status={reply['content']['status']}"
            f" ({reply['content']['ename']})"
        )
        while True:
            reply = client.get_control_msg(timeout=30)
            if reply["parent_header"].get("msg_id") == request["header"]["msg_id"]:
                break
        print(f"  [control] interrupt_reply -> {reply['content']}")
        run_cell(client, "print('marker =', marker)")
        run_cell(client, "'the next cell is unharmed'")

        section("5. heartbeat answers while a cell is running")
        import zmq

        cfg = json.load(open(conn_path, encoding="utf-8"))
        msg_id = client.execute("import time; time.sleep(30)")
        while True:
            msg = client.get_iopub_msg(timeout=30)
            if (
                msg["parent_header"].get("msg_id") == msg_id
                and msg["msg_type"] == "execute_input"
            ):
                break
        ping = zmq.Context.instance().socket(zmq.REQ)
        ping.linger = 0
        ping.connect(f"tcp://127.0.0.1:{cfg['hb_port']}")
        started = time.monotonic()
        ping.send(b"are-you-alive")
        pong = ping.recv() if ping.poll(5000) else None
        ping.close(0)
        print(f"  heartbeat replied {pong!r} in {time.monotonic() - started:.3f}s")
        print("  >>> interrupting the sleeper to leave the kernel clean")
        client.control_channel.send(client.session.msg("interrupt_request", {}))
        while True:
            msg = client.get_iopub_msg(timeout=30)
            if (
                msg["parent_header"].get("msg_id") == msg_id
                and msg["msg_type"] == "status"
                and msg["content"]["execution_state"] == "idle"
            ):
                break
        while True:
            reply = client.get_shell_msg(timeout=30)
            if reply["parent_header"].get("msg_id") == msg_id:
                break
        print(f"  [shell] execute_reply   -> status={reply['content']['status']}")

        section("6. shutdown: process exits, ports are released")
        reply = client.shutdown(reply=True, timeout=10)
        print(f"  shutdown_reply: {reply['content']}")
        proc.wait(timeout=10)
        print(f"  kernel process exited with code {proc.returncode}")
        probe = socket.socket()
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", cfg["shell_port"]))
        probe.close()
        print("  shell port is free again")
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
    print("\ndemo finished.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
