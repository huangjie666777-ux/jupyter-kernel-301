# purekernel — 纯 Python 实现的 Jupyter 协议 5.3 后端内核

一个不启动、不代理 ipykernel 的纯后端 Jupyter 内核：直接基于 ZeroMQ
套接字实现 Jupyter 协议 5.3 的有线格式，消息编解码与 HMAC 签名复用
`jupyter_client.session.Session`。可用任意标准 Jupyter 客户端
（`jupyter_client`、JupyterLab、Notebook 等）连接执行 Python 单元，
变量在单元间保留。

## 目录结构与模块协作

```
purekernel/
  __main__.py     进程监管：参数解析、日志、退出码（python -m purekernel）
  connection.py   连接 JSON 的读取与校验，生成各通道 endpoint
  kernel.py       认证与路由（复用 jupyter_client Session）、shell 主循环、
                  control 线程、消息分发、执行计数、关闭监管
  execution.py    执行上下文：共享命名空间、末尾表达式求值、stdout/stderr
                  实时捕获、stdin 拒绝、回溯修剪
  interrupts.py   中断协调：代际计数保证信号只命中"请求时的那个单元"
  iopub.py        IOPub 发布：专用线程独占 PUB 套接字，队列转发保序
  heartbeat.py    心跳：独立线程 REP 回显，长运算期间照常应答
tests/
  test_kernel.py  14 个集成测试：真实子进程 + 真实 BlockingKernelClient
demo.py           端到端演示：启动、执行、异常、中断、心跳、关闭
```

线程模型（每个 ZeroMQ 套接字只被一个线程触碰）：

| 线程 | 套接字 | 职责 |
| --- | --- | --- |
| 主线程 | shell / stdin ROUTER | 消息分发并**串行执行单元**（信号处理作用于用户代码） |
| control 线程 | control ROUTER | interrupt / shutdown / kernel_info，执行期间仍可响应 |
| heartbeat 线程 | hb REP | 回显 ping，长运算时心跳不断 |
| iopub 线程 | iopub PUB | 经 FIFO 队列转发 busy/input/stream/result/error/idle |

工作线程通过 `pthread_sigmask` 屏蔽 SIGINT/SIGTERM，信号只会投递到
主线程；control 线程用 `pthread_kill` 把 SIGINT 精确送达主线程，从而
`time.sleep` 之类的阻塞调用也能被 `KeyboardInterrupt` 打断。

## 运行

以下命令均在仓库根目录执行，解释器用 `.venv`（Python 3.14.4，
pyzmq 27.1.0，jupyter_client 8.6.3）。

### 1. 启动内核

```bash
# 先准备一个连接文件（端口可任选空闲端口）
cat > /tmp/connection.json <<'JSON'
{
  "transport": "tcp",
  "ip": "127.0.0.1",
  "signature_scheme": "hmac-sha256",
  "key": "demo-secret",
  "shell_port": 55101, "control_port": 55102, "iopub_port": 55103,
  "stdin_port": 55104, "hb_port": 55105,
  "kernel_name": "purekernel"
}
JSON

# 启动（前台运行，日志走 stderr；-v 输出调试日志）
.venv/bin/python -m purekernel -f /tmp/connection.json
```

用真实 Jupyter 客户端连接：

```python
from jupyter_client import BlockingKernelClient

client = BlockingKernelClient()
client.connection_file = "/tmp/connection.json"
client.load_connection_file()
client.start_channels()
client.wait_for_ready(timeout=10)

client.execute("a = 6 * 7\nprint(a)")     # 单元串行执行，变量保留
client.shutdown()                          # 内核回收套接字并退出
```

### 2. 端到端演示（执行 / 异常 / 中断 / 心跳 / 关闭）

```bash
.venv/bin/python demo.py
```

演示脚本会自建临时连接文件、启动内核子进程，并用
`BlockingKernelClient` 完整走一遍：kernel_info 握手、流式 stdout 与
`text/plain` 结果、`ZeroDivisionError` 异常后内核继续可用、control
通道中断 60 秒长单元（`marker` 变量保留、后续单元不受误伤）、长运算
期间心跳应答、shutdown 后进程退出且端口释放。

### 3. 测试

```bash
.venv/bin/python -m pytest tests/ -v
```

14 个集成测试覆盖：启动与 kernel_info（shell 与 control 双通道）、
执行结果与流式输出、共享命名空间、`silent`/`store_history` 计数语义、
异常报告与恢复、语法错误、stdin 拒绝（不挂起）、长单元中断与命名空间
保留、空闲中断无副作用、执行期间心跳、坏签名/畸形消息拒绝、多客户端
路由不串、shutdown 回收进程与端口。

## 协议行为说明

- **认证与路由**：所有请求经 `Session` 验证 HMAC 签名，坏签名与畸形
  帧直接丢弃并记日志；回复原样带回请求的路由标识（idents）与
  `parent_header`，多客户端并发时回复各回各家。
- **执行**：单元在主线程串行执行、共享一个命名空间；末尾为裸表达式
  时以 `repr()` 作为 `text/plain` 的 `execute_result` 发布。
- **IOPub 序列**：`status:busy` → `execute_input` → `stream` /
  `execute_result` / `error` → `status:idle`，随后 shell 返回
  `execute_reply`（顺序由单队列保证）。
- **计数语义**：`store_history=True` 时 `execution_count` 自增；
  `silent=True` 强制 `store_history=False`，抑制该单元全部 IOPub 输出
  （仅保留 busy/idle），计数不动，回复照常返回。
- **异常**：`error` 消息与 `execute_reply` 携带 `ename`/`evalue`/
  `traceback`（已修剪内核内部帧）；异常不影响后续执行。
- **中断**：control 通道收到 `interrupt_request` 后，向主线程投递
  SIGINT，当前单元以 `KeyboardInterrupt` 错误结束，已建立的变量保留；
  代际计数保证迟到的信号被吞掉——已完成单元不会产生第二个终态，
  后续单元也不会被误伤。空闲时收到中断请求只是回执，无副作用。
- **关闭**：`shutdown_request`（shell 或 control）立即回复
  `shutdown_reply`，随后中断当前单元、回收全部套接字并退出进程；
  若用户代码吞掉中断拒不退出，看门狗 3 秒后强制结束进程，绝不挂死。
- **心跳**：独立线程即时回显，长运算期间客户端活性检测不受影响。

## 有意的限制

- 仅执行可信本机代码：**无沙盒**。
- 不支持富媒体：结果只有 `text/plain`。
- 不支持 `user_expressions`：回复中恒为 `{}`。
- 不做 stdin 交互：单元内 `input()` 立即抛 `EOFError`，不会挂起内核。
- 不支持顶层 `await`（无事件循环）；`complete`/`inspect`/`history`/
  `debug` 等请求类型未实现，会被记录并忽略。
- 中断依赖 Unix 信号语义（`pthread_kill`/`pthread_sigmask`），面向
  Linux/macOS。
