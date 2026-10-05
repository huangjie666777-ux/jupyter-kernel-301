"""purekernel -- a pure-Python Jupyter protocol 5.3 kernel backend.

The kernel speaks the Jupyter wire protocol over ZeroMQ directly (no
ipykernel is started or proxied).  Message framing, JSON codecs and HMAC
signing are reused from :class:`jupyter_client.session.Session`.
"""

__version__ = "1.0.0"

from .connection import ConnectionInfo
from .kernel import Kernel

__all__ = ["ConnectionInfo", "Kernel", "__version__"]
