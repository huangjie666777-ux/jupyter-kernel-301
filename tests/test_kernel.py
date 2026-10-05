"""minikernel 集成测试: 用真实 jupyter_client 客户端驱动内核子进程。"""
from __future__ import annotations

import os
import random
import subprocess
import sys
import time
from pathlib import Path

import pytest
import zmq
from jupyter_client.blocking import BlockingKernelClient
from jupyter_client.connect import write_connection_file
from jupyter_client.session import Session

REPO_ROOT = Path(__file__).resolve().parents[1]


class KernelHandle:
    def __init__(self, proc, client, conn_file):
        self.proc = proc
        self.client = client
        self.conn_file = conn_file


def _spawn(tmp_path) -> KernelHandle:
    conn_file = str(tmp_path / "connection.json")
    write_connection_file(conn_file, ip="127.0.0.1", key=b"minikernel-test-key")
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [sys.executable, "-m", "minikernel", "-f", conn_file],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    client = BlockingKernelClient()
    client.load_connection_file(conn_file)
    client.start_channels()
    try:
        client.wait_for_ready(timeout=20)
    except Exception:
        stderr = ""
        if proc.poll() is not None and proc.stderr is not None:
            stderr = proc.stderr.read()
        client.stop_channels()
        proc.kill()
        raise AssertionError(f"内核未能就绪 (returncode={proc.poll()})\n{stderr}")
    return KernelHandle(proc, client, conn_file)


@pytest.fixture
def kernel(tmp_path):
    handle = _spawn(tmp_path)
    try:
        yield handle
    finally:
        handle.client.stop_channels()
        if handle.proc.poll() is None:
            handle.proc.terminate()
            try:
                handle.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                handle.proc.kill()


def drain_iopub(client, parent_id, timeout=10.0):
    """收集某个 parent msg_id 的 IOPub 消息, 直到 idle。"""
    msgs = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            msg = client.get_iopub_msg(timeout=max(0.1, deadline - time.monotonic()))
        except Exception:
            break
        if msg["parent_header"].get("msg_id") != parent_id:
            continue
        msgs.append(msg)
        if msg["msg_type"] == "status" and msg["content"]["execution_state"] == "idle":
            break
    return msgs


def interrupt(client, timeout=5.0):
    """经 control 通道发送 interrupt_request 并等待 interrupt_reply。"""
    client.control_channel.send(client.session.msg("interrupt_request", {}))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msg = client.get_control_msg(timeout=max(0.1, deadline - time.monotonic()))
        if msg["msg_type"] == "interrupt_reply":
            return msg
    raise AssertionError("未收到 interrupt_reply")


# ---------------------------------------------------------------- 启动


def test_startup_and_heartbeat(kernel):
    """进程启动后绑定各通道, 心跳可用。"""
    assert kernel.proc.poll() is None
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if kernel.client.hb_channel.is_beating():
            break
        time.sleep(0.1)
    else:
        raise AssertionError("心跳未应答")


# ---------------------------------------------------------------- 基础


def test_kernel_info(kernel):
    kernel.client.kernel_info()
    msg = kernel.client.get_shell_msg(timeout=5)
    assert msg["msg_type"] == "kernel_info_reply"
    content = msg["content"]
    assert content["status"] == "ok"
    assert content["protocol_version"] == "5.3"
    assert content["implementation"] == "minikernel"
    assert content["language_info"]["name"] == "python"


def test_execute_result_and_count(kernel):
    c = kernel.client
    mid = c.execute("1 + 1")
    reply = c.get_shell_msg(timeout=10)
    assert reply["msg_type"] == "execute_reply"
    assert reply["parent_header"]["msg_id"] == mid
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["execution_count"] == 1

    msgs = drain_iopub(c, mid)
    types = [m["msg_type"] for m in msgs]
    assert types[0] == "status" and msgs[0]["content"]["execution_state"] == "busy"
    assert "execute_input" in types
    results = [m for m in msgs if m["msg_type"] == "execute_result"]
    assert len(results) == 1
    assert results[0]["content"]["data"]["text/plain"] == "2"
    assert results[0]["content"]["execution_count"] == 1
    assert types[-1] == "status" and msgs[-1]["content"]["execution_state"] == "idle"


def test_shared_namespace(kernel):
    c = kernel.client
    c.execute("x = 41")
    c.get_shell_msg(timeout=10)
    mid = c.execute("x + 1")
    reply = c.get_shell_msg(timeout=10)
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["execution_count"] == 2
    results = [m for m in drain_iopub(c, mid) if m["msg_type"] == "execute_result"]
    assert results[0]["content"]["data"]["text/plain"] == "42"


def test_stream_stdout_stderr(kernel):
    c = kernel.client
    mid = c.execute("import sys\nprint('hello-out')\nprint('hello-err', file=sys.stderr)")
    reply = c.get_shell_msg(timeout=10)
    assert reply["content"]["status"] == "ok"
    streams = [m for m in drain_iopub(c, mid) if m["msg_type"] == "stream"]
    out = "".join(m["content"]["text"] for m in streams if m["content"]["name"] == "stdout")
    err = "".join(m["content"]["text"] for m in streams if m["content"]["name"] == "stderr")
    assert out == "hello-out\n"
    assert err == "hello-err\n"


def test_exception_and_survival(kernel):
    c = kernel.client
    mid = c.execute("1 / 0")
    reply = c.get_shell_msg(timeout=10)
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "ZeroDivisionError"
    assert "division by zero" in reply["content"]["evalue"]
    assert any("ZeroDivisionError" in line for line in reply["content"]["traceback"])

    errors = [m for m in drain_iopub(c, mid) if m["msg_type"] == "error"]
    assert len(errors) == 1
    assert errors[0]["content"]["ename"] == "ZeroDivisionError"

    # 异常后内核仍能执行, 计数继续
    mid2 = c.execute("6 * 7")
    reply2 = c.get_shell_msg(timeout=10)
    assert reply2["content"]["status"] == "ok"
    assert reply2["content"]["execution_count"] == 2
    results = [m for m in drain_iopub(c, mid2) if m["msg_type"] == "execute_result"]
    assert results[0]["content"]["data"]["text/plain"] == "42"


def test_silent_and_store_history(kernel):
    c = kernel.client
    # silent: 抑制输出、不计数
    mid = c.execute("print('hidden')\n'also-hidden'", silent=True)
    reply = c.get_shell_msg(timeout=10)
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["execution_count"] == 0
    msgs = drain_iopub(c, mid)
    leaked = [m for m in msgs if m["msg_type"] in ("execute_input", "stream", "execute_result", "error")]
    assert leaked == []

    # store_history=False: 输出照常, 但不计数
    mid = c.execute("'shown'", store_history=False)
    reply = c.get_shell_msg(timeout=10)
    assert reply["content"]["execution_count"] == 0
    results = [m for m in drain_iopub(c, mid) if m["msg_type"] == "execute_result"]
    assert results[0]["content"]["data"]["text/plain"] == "'shown'"

    # 正常单元从 1 开始计数
    mid = c.execute("1")
    reply = c.get_shell_msg(timeout=10)
    assert reply["content"]["execution_count"] == 1
    drain_iopub(c, mid)


def test_stdin_rejected(kernel):
    c = kernel.client
    mid = c.execute("input('give me: ')", allow_stdin=True)
    reply = c.get_shell_msg(timeout=10)
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "RuntimeError"
    drain_iopub(c, mid)
    # 内核没有挂起
    c.execute("'alive'")
    reply = c.get_shell_msg(timeout=10)
    assert reply["content"]["status"] == "ok"


# ---------------------------------------------------------------- 中断


def test_interrupt_long_running_cell(kernel):
    c = kernel.client
    mid = c.execute("import time\nkept = 1234\ntime.sleep(60)\nkept = 0")
    # 等单元真正开始执行
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        msg = c.get_iopub_msg(timeout=10)
        if msg["parent_header"].get("msg_id") == mid and msg["msg_type"] == "execute_input":
            break
    # 长运算期间心跳必须应答
    assert c.hb_channel.is_beating()

    t0 = time.monotonic()
    reply = interrupt(c)
    assert reply["content"]["status"] == "ok"

    shell = c.get_shell_msg(timeout=10)
    elapsed = time.monotonic() - t0
    assert elapsed < 10, "中断没有及时结束当前单元"
    assert shell["parent_header"]["msg_id"] == mid
    assert shell["content"]["status"] == "error"
    assert shell["content"]["ename"] == "KeyboardInterrupt"
    drain_iopub(c, mid)

    # 已有变量保留, 随后单元不受影响
    mid2 = c.execute("kept + 1")
    reply2 = c.get_shell_msg(timeout=10)
    assert reply2["content"]["status"] == "ok"
    results = [m for m in drain_iopub(c, mid2) if m["msg_type"] == "execute_result"]
    assert results[0]["content"]["data"]["text/plain"] == "1235"


def test_interrupt_when_idle_is_noop(kernel):
    c = kernel.client
    reply = interrupt(c)
    assert reply["content"]["status"] == "ok"
    mid = c.execute("2 + 2")
    shell = c.get_shell_msg(timeout=10)
    assert shell["content"]["status"] == "ok"
    results = [m for m in drain_iopub(c, mid) if m["msg_type"] == "execute_result"]
    assert results[0]["content"]["data"]["text/plain"] == "4"


def test_interrupt_completion_race_single_terminal_state(kernel):
    """中断与完成竞争: 每个单元必须恰好一个终态, 且不误伤随后单元。"""
    c = kernel.client
    for i in range(15):
        mid = c.execute(f"import time; time.sleep(0.05); {i}")
        time.sleep(random.uniform(0.0, 0.12))
        interrupt(c)
        shell = c.get_shell_msg(timeout=10)
        assert shell["parent_header"]["msg_id"] == mid
        assert shell["content"]["status"] in ("ok", "error")
        drain_iopub(c, mid)
        # 不应有第二个终态(重复 execute_reply)
        with pytest.raises(Exception):
            c.get_shell_msg(timeout=0.3)
    mid = c.execute("'still-alive'")
    shell = c.get_shell_msg(timeout=10)
    assert shell["content"]["status"] == "ok"
    drain_iopub(c, mid)


# ---------------------------------------------------------------- 认证与路由


def test_bad_signature_rejected(kernel):
    c = kernel.client
    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"tcp://127.0.0.1:{c.shell_port}")
    evil = Session(key=b"forged-key", signature_scheme="hmac-sha256")
    evil.send(sock, "kernel_info_request", {})
    assert not sock.poll(1500), "坏签名消息不应得到回复"
    sock.close()
    ctx.term()

    # 内核没有被坏消息影响
    c.kernel_info()
    reply = c.get_shell_msg(timeout=5)
    assert reply["msg_type"] == "kernel_info_reply"


def test_multi_client_routing(kernel, tmp_path):
    c1 = kernel.client
    c2 = BlockingKernelClient()
    c2.load_connection_file(kernel.conn_file)
    c2.start_channels()
    try:
        c2.wait_for_ready(timeout=20)
        m1 = c1.execute("import time; time.sleep(1.0); 'first'")
        m2 = c2.execute("'second'")
        r1 = c1.get_shell_msg(timeout=20)
        r2 = c2.get_shell_msg(timeout=20)
        # 回复按路由回到各自客户端, 父消息关联正确
        assert r1["parent_header"]["msg_id"] == m1
        assert r2["parent_header"]["msg_id"] == m2
        assert r1["content"]["status"] == "ok"
        assert r2["content"]["status"] == "ok"
        # 串行执行: c1 的单元先完成
        drain_iopub(c1, m1)
        drain_iopub(c2, m2)
    finally:
        c2.stop_channels()


# ---------------------------------------------------------------- 关闭


def test_shutdown_request(kernel):
    c = kernel.client
    c.shutdown()
    deadline = time.monotonic() + 5
    reply = None
    while time.monotonic() < deadline:
        msg = c.get_control_msg(timeout=5)
        if msg["msg_type"] == "shutdown_reply":
            reply = msg
            break
    assert reply is not None
    assert reply["content"]["status"] == "ok"
    kernel.proc.wait(timeout=10)
    assert kernel.proc.returncode == 0
