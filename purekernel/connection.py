"""Connection-file handling.

A Jupyter connection file is a small JSON document describing how clients
reach the kernel, e.g.::

    {
      "transport": "tcp",
      "ip": "127.0.0.1",
      "signature_scheme": "hmac-sha256",
      "key": "shared-secret",
      "shell_port": 50101, "control_port": 50102, "iopub_port": 50103,
      "stdin_port": 50104, "hb_port": 50105
    }

The kernel binds every channel on the loopback address taken from this
file; nothing here ever listens on a non-local interface.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

_REQUIRED_PORTS = ("shell_port", "control_port", "iopub_port", "stdin_port", "hb_port")


@dataclass(frozen=True)
class ConnectionInfo:
    """Parsed, validated connection parameters."""

    transport: str
    ip: str
    signature_scheme: str
    key: bytes
    shell_port: int
    control_port: int
    iopub_port: int
    stdin_port: int
    hb_port: int

    @classmethod
    def from_file(cls, path: str) -> "ConnectionInfo":
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            raise ValueError(f"connection file {path!r} does not contain a JSON object")

        scheme = str(data.get("signature_scheme", "hmac-sha256"))
        if not scheme.startswith("hmac-"):
            raise ValueError(f"unsupported signature_scheme {scheme!r}")

        ports = {}
        for name in _REQUIRED_PORTS:
            if name not in data:
                raise ValueError(f"connection file {path!r} is missing {name!r}")
            ports[name] = int(data[name])

        return cls(
            transport=str(data.get("transport", "tcp")),
            ip=str(data.get("ip", "127.0.0.1")),
            signature_scheme=scheme,
            key=str(data.get("key") or "").encode("utf-8"),
            **ports,
        )

    def endpoint(self, port: int) -> str:
        return f"{self.transport}://{self.ip}:{port}"

    @property
    def shell_endpoint(self) -> str:
        return self.endpoint(self.shell_port)

    @property
    def control_endpoint(self) -> str:
        return self.endpoint(self.control_port)

    @property
    def iopub_endpoint(self) -> str:
        return self.endpoint(self.iopub_port)

    @property
    def stdin_endpoint(self) -> str:
        return self.endpoint(self.stdin_port)

    @property
    def hb_endpoint(self) -> str:
        return self.endpoint(self.hb_port)
