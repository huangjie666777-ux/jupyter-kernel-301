"""认证与路由: 复用 jupyter_client Session 的编解码与 HMAC 校验。"""
from __future__ import annotations

import zmq
from jupyter_client.session import Session


def new_session(key: bytes, signature_scheme: str) -> Session:
    """新建一个 Session。

    每个通道线程持有独立实例(共享同一把密钥), 避免跨线程共享
    digest_history 等可变状态。
    """
    return Session(key=key, signature_scheme=signature_scheme)


class BadMessage(Exception):
    """签名错误、重放或格式非法的消息。"""


def recv_request(session: Session, socket: zmq.Socket):
    """接收并校验一个请求, 返回 (idents, msg)。

    idents 是 ROUTER 套接字上的路由前缀, 回复时必须原样带回,
    多客户端下才不会串路由。签名非法时抛 BadMessage, 由调用方丢弃。
    """
    frames = socket.recv_multipart()
    try:
        idents, message_frames = session.feed_identities(frames)
        msg = session.deserialize(message_frames)
    except (ValueError, TypeError) as exc:
        raise BadMessage(str(exc)) from exc
    return idents, msg
