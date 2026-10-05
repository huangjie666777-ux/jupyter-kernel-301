"""连接文件解析: 端口、密钥与环回地址绑定。"""
from __future__ import annotations

import ipaddress
import json
from dataclasses import dataclass
from pathlib import Path

LOOPBACK = "127.0.0.1"


@dataclass(frozen=True)
class ConnectionInfo:
    """连接 JSON 的强类型视图。"""

    ip: str
    transport: str
    shell_port: int
    control_port: int
    iopub_port: int
    stdin_port: int
    hb_port: int
    key: bytes
    signature_scheme: str

    def endpoint(self, port: int) -> str:
        return f"{self.transport}://{self.ip}:{port}"


def _as_loopback(ip: str) -> str:
    """内核只监听环回地址; 空、通配或非环回地址一律回退到 127.0.0.1。"""
    if not ip:
        return LOOPBACK
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return LOOPBACK
    return ip if addr.is_loopback else LOOPBACK


def load_connection_file(path: str | Path) -> ConnectionInfo:
    """读取 Jupyter 连接 JSON 并校验必需字段。"""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    transport = data.get("transport", "tcp")
    if transport != "tcp":
        raise ValueError(f"仅支持 tcp 传输, 收到: {transport!r}")
    key = data.get("key", "")
    if isinstance(key, str):
        key = key.encode("utf-8")
    return ConnectionInfo(
        ip=_as_loopback(data.get("ip", "")),
        transport=transport,
        shell_port=int(data["shell_port"]),
        control_port=int(data["control_port"]),
        iopub_port=int(data["iopub_port"]),
        stdin_port=int(data["stdin_port"]),
        hb_port=int(data["hb_port"]),
        key=key,
        signature_scheme=data.get("signature_scheme", "hmac-sha256"),
    )
