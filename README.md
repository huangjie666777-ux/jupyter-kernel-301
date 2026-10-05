# minikernel — 纯 Python Jupyter 协议 5.3 后端内核

面向研发场景的极简 Jupyter 内核: 用 Jupyter 客户端执行 Python 单元并保留变量。
仅依赖 `pyzmq` 与 `jupyter_client`(复用其 `Session` 编解码与 HMAC 校验),
**不启动、不代理 ipykernel**, 内核协议 5.3 由本项目直接实现。

## 运行环境

- Python 3.14.4
- pyzmq 27.1.0
- jupyter_client 8.6.3

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.lock.txt
```

## 快速开始

```bash
# 1. 准备连接文件(端口、密钥)
.venv/bin/python - <<'PY'
from jupyter_client.connect import write_connection_file
write_connection_file("connection.json", ip="127.0.0.1", key=b"my-secret")
PY

# 2. 启动内核(只在环回地址绑定)
.venv/bin/python -m minikernel -f connection.json

# 3. 用任意 Jupyter 客户端连接, 例如:
.venv/bin/jupyter console --existing connection.json
```

## 测试与演示

```bash
# 集成测试: 真实 jupyter_client 驱动内核子进程
.venv/bin/python -m pytest

# 演示: 执行、异常、中断、关闭的完整过程
.venv/bin/python examples/demo.py
```

## 目录结构(跨文件协作)

```
minikernel/
  __main__.py     入口: python -m minikernel -f connection.json
  connection.py   连接 JSON 解析; 强制环回地址绑定
  security.py     认证与路由: 复用 jupyter_client Session 的编解码与 HMAC 校验
  kernel.py       内核主体: 通道线程、串行执行循环、消息分发、关闭
  execution.py    执行上下文: 共享命名空间、输出转发、可中断执行
  iopub.py        输出发布: status / execute_input / stream / execute_result / error
  heartbeat.py    心跳线程: REP 回显, 与执行完全解耦
  supervisor.py   进程监管: 信号处理、生命周期、资源回收
tests/test_kernel.py   集成测试(真实 jupyter_client)
examples/demo.py       演示脚本
```

协作关系: `supervisor` 驱动 `kernel` 的生命周期; `kernel` 经 `security`
收发并校验消息, 把执行请求交给主线程的 `execution` 执行上下文, 执行产出经
`iopub` 实时发布; `heartbeat` 独立应答存活探测。

## 协议行为

- **通道**: shell / control / iopub / stdin / heartbeat 全部绑定在环回地址。
  stdin 仅绑定, 内核永不发起 `input_request`; 用户代码里的 `input()` 立即报错,
  不会挂起内核。
- **认证**: 所有请求经 HMAC 签名校验, 坏签名/重放消息直接丢弃; 回复沿原路由
  idents 返回并保留父消息关联, 多客户端不串路由。
- **执行**: 单元串行执行、共享命名空间; 末尾表达式以 `text/plain` 返回
  `execute_result`; stdout/stderr 按行实时发布; 异常返回 `ename`/`evalue`/
  `traceback`, 之后内核继续可用。
- **消息流**: 每个请求按协议发布 `busy → (execute_input → stream/result/error)
  → idle`; `execution_count` 遵守 `silent` 与 `store_history` 语义
  (`silent` 强制 `store_history=False` 并抑制该单元的全部 IOPub 输出)。
- **中断**: control 通道 `interrupt_request` 借助 SIGINT 把 `KeyboardInterrupt`
  投递进主线程当前单元 —— 只结束当前单元、保留已有变量; SIGINT 处理器只在
  中断确实瞄准当前单元时才抛出, 迟到信号直接吞掉, 不误伤随后单元。单元的
  终态(`execute_reply` + `idle`)只由执行线程发出, 每单元恰好一次,
  完成与中断竞争不会产生重复终态。
- **关闭**: `shutdown_request`(shell 或 control)回复后回收线程与套接字并退出;
  3 秒看门狗兜底保证进程一定被回收。SIGTERM 走同样的优雅关闭路径。

## 设计说明

- **执行与通信分离**: 主线程串行执行单元; shell / control / heartbeat 各自
  独立线程; IOPub 经带锁的发布器发送。长运算期间心跳与控制通道照常响应。
- **为什么用 SIGINT 而不是跨线程异步异常**: Python 信号处理器只在主线程
  执行, 配合"目标单元号"武装检查, 可以精确命中当前单元; `time.sleep` 等
  阻塞调用在主线程可被信号立即打断。
- **安全模型**: 仅面向可信本机代码 —— 不做沙盒; 只监听环回地址; 依赖连接
  文件密钥做消息认证。

## 已知限制(明确不支持)

- 用户表达式(`user_expressions` 一律返回 `{}`)、富媒体(只发 `text/plain`)
- comm / debugger / 完整代码分析(`is_complete` 恒返回 `complete`)
- IPython 魔法命令(内核执行的是纯 Python)
- stdin 交互输入
