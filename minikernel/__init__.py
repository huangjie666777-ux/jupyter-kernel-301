"""minikernel — 纯 Python 实现的 Jupyter 协议 5.3 后端内核。

仅依赖 pyzmq 与 jupyter_client(复用其 Session 编解码与 HMAC 校验),
不启动、不代理 ipykernel。
"""

__version__ = "0.1.0"
