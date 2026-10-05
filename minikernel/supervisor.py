"""进程监管: 信号处理、生命周期驱动与资源回收。"""
from __future__ import annotations

import logging
import signal

from .kernel import Kernel

log = logging.getLogger("minikernel")


class Supervisor:
    """监管内核进程。

    - SIGINT:  转交执行器, 仅当中断瞄准当前单元时生效(与 control 通道同语义)
    - SIGTERM: 置停止标记并借道 SIGINT 打断当前单元, 走优雅关闭
    - 退出时:  回收通道线程、套接字与 zmq 上下文
    """

    def __init__(self, kernel: Kernel) -> None:
        self.kernel = kernel

    def run(self) -> None:
        self.kernel.start()
        self._install_signal_handlers()
        log.info("minikernel 就绪, 等待请求")
        try:
            self.kernel.exec_loop()
        except KeyboardInterrupt:
            pass  # 关闭路径上迟到的 SIGINT
        finally:
            try:
                self.kernel.close()
            except KeyboardInterrupt:
                pass

    def _install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.kernel.executor.handle_sigint)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame) -> None:
        # 信号处理器内不做任何可能取锁的操作(日志除外风险高, 故省略)
        self.kernel.stopping.set()
        self.kernel.executor.interrupt_from_signal_handler()
