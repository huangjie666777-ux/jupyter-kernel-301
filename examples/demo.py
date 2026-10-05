"""用真实 jupyter_client 客户端演示 minikernel: 执行、异常、中断与关闭。

用法:  .venv/bin/python examples/demo.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from jupyter_client.blocking import BlockingKernelClient
from jupyter_client.connect import write_connection_file

REPO_ROOT = Path(__file__).resolve().parents[1]


def show_iopub(client, parent_id, timeout=5.0):
    """打印某单元的 IOPub 消息流, 直到 idle。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msg = client.get_iopub_msg(timeout=max(0.1, deadline - time.monotonic()))
        if msg["parent_header"].get("msg_id") != parent_id:
            continue
        kind = msg["msg_type"]
        content = msg["content"]
        if kind == "status":
            print(f"    iopub/status          -> {content['execution_state']}")
        elif kind == "execute_input":
            print(f"    iopub/execute_input   -> count={content['execution_count']}")
        elif kind == "stream":
            print(f"    iopub/stream[{content['name']}]  -> {content['text']!r}")
        elif kind == "execute_result":
            print(f"    iopub/execute_result  -> {content['data']['text/plain']}")
        elif kind == "error":
            print(f"    iopub/error           -> {content['ename']}: {content['evalue']}")
        if kind == "status" and content["execution_state"] == "idle":
            break


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="minikernel-demo-")
    conn_file = os.path.join(tmp, "connection.json")
    write_connection_file(conn_file, ip="127.0.0.1", key=b"demo-key")

    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [sys.executable, "-m", "minikernel", "-f", conn_file],
        cwd=REPO_ROOT,
        env=env,
    )
    client = BlockingKernelClient()
    client.load_connection_file(conn_file)
    client.start_channels()
    print("== 启动内核并等待就绪(心跳 + kernel_info + iopub) ==")
    client.wait_for_ready(timeout=20)
    print(f"   内核进程 pid={proc.pid} 已就绪\n")

    try:
        print("== 1. 执行单元: 共享命名空间 + 末尾表达式 text/plain ==")
        mid = client.execute("a = 21")
        client.get_shell_msg(timeout=5)
        show_iopub(client, mid)
        mid = client.execute("a * 2")
        reply = client.get_shell_msg(timeout=5)
        show_iopub(client, mid)
        print(f"    shell/execute_reply   -> status={reply['content']['status']} count={reply['content']['execution_count']}\n")

        print("== 2. 实时标准输出 ==")
        mid = client.execute("import time\nfor i in range(3):\n    print(f'tick {i}')\n    time.sleep(0.3)")
        client.get_shell_msg(timeout=10)
        show_iopub(client, mid)
        print()

        print("== 3. 异常: 返回类型/信息/回溯, 内核继续存活 ==")
        mid = client.execute("1 / 0")
        reply = client.get_shell_msg(timeout=5)
        show_iopub(client, mid)
        print(f"    shell/execute_reply   -> {reply['content']['status']} {reply['content']['ename']}: {reply['content']['evalue']}")
        mid = client.execute("'kernel still alive'")
        client.get_shell_msg(timeout=5)
        show_iopub(client, mid)
        print()

        print("== 4. 中断: control 通道打断 60s 长运算 ==")
        mid = client.execute("import time\nkept = 1234\ntime.sleep(60)\nkept = 0")
        while True:  # 等单元开始执行
            msg = client.get_iopub_msg(timeout=10)
            if msg["parent_header"].get("msg_id") == mid and msg["msg_type"] == "execute_input":
                break
        print(f"    长运算期间心跳: {'正常' if client.hb_channel.is_beating() else '异常'}")
        t0 = time.monotonic()
        client.control_channel.send(client.session.msg("interrupt_request", {}))
        client.get_control_msg(timeout=5)
        reply = client.get_shell_msg(timeout=10)
        print(f"    中断耗时 {time.monotonic() - t0:.2f}s -> status={reply['content']['status']} ename={reply['content']['ename']}")
        show_iopub(client, mid)
        mid = client.execute("kept + 1")
        client.get_shell_msg(timeout=5)
        show_iopub(client, mid)
        print("    变量保留, 随后单元正常\n")
    finally:
        print("== 5. 关闭: shutdown_request 回收进程 ==")
        client.shutdown()
        client.get_control_msg(timeout=5)
        proc.wait(timeout=10)
        print(f"   内核进程已退出, returncode={proc.returncode}")
        client.stop_channels()


if __name__ == "__main__":
    main()
