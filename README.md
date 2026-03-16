# myRaftStore

基于 C++17 从零实现的分布式 KV 存储，包含自研 RPC 框架（myRPC）、Raft 共识协议和异步日志系统。

---

## 目录

- [项目概览](#项目概览)
- [架构图](#架构图)
- [核心模块](#核心模块)
  - [myRPC 框架](#myrpc-框架)
  - [Raft 共识协议](#raft-共识协议)
  - [分布式 KV 存储](#分布式-kv-存储)
  - [协程调度器](#协程调度器)
  - [连接池](#连接池)
  - [异步日志](#异步日志)
- [目录结构](#目录结构)
- [依赖](#依赖)
- [构建与运行](#构建与运行)
- [性能测试](#性能测试)

---

## 项目概览

| 特性 | 说明 |
|------|------|
| 语言标准 | C++17 |
| 网络模型 | epoll ET + EPOLLONESHOT + 非阻塞 IO |
| RPC 序列化 | Protocol Buffers |
| 共识协议 | Raft（领导选举、日志复制、快照、InstallSnapshot）|
| 并发模型 | 线程池 + 协程批量调度（ucontext）|
| 持久化 | 二进制日志 + 快照（CRC32 校验 + fdatasync）|
| 日志系统 | 异步写入，`write()` 直写内核 page cache，SIGKILL 安全 |
| 连接管理 | 每端点连接池（惰性健康检测，复用 TCP 连接）|

---

## 架构图

```
┌─────────────────────────────────────────────────┐
│                   Client                        │
│              RaftClient (raft_client.h)         │
│   PUT / GET / DELETE / LIST + 退避重试          │
└─────────────────┬───────────────────────────────┘
                  │ RPC call
┌─────────────────▼───────────────────────────────┐
│                 myRPC 框架                       │
│  ┌────────────────────────────────────────────┐ │
│  │  RpcServer                                 │ │
│  │  epoll ET + EPOLLONESHOT                   │ │
│  │  ThreadPool ──► Coroutine Scheduler        │ │
│  │  ConnectionPool（每端点最多 10 连接）       │ │
│  └────────────────────────────────────────────┘ │
└─────────────────┬───────────────────────────────┘
                  │ 方法分发（Protobuf 反射）
┌─────────────────▼───────────────────────────────┐
│              RaftServiceImpl                    │
│   RequestVote / AppendEntries / ClientStorage   │
│   InstallSnapshot                               │
└─────────────────┬───────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────┐
│                RaftNode                         │
│  ┌──────────┐ ┌──────────┐ ┌─────────────────┐ │
│  │ 领导选举  │ │ 日志复制  │ │   快照机制       │ │
│  │ 退避重试  │ │ AppendEnt│ │  CRC32 校验     │ │
│  └──────────┘ └──────────┘ └─────────────────┘ │
│  ┌─────────────────────────────────────────────┐│
│  │     持久化层：raft_state + raft_log          ││
│  │     fdatasync 保证 crash-safe               ││
│  └─────────────────────────────────────────────┘│
└─────────────────────────────────────────────────┘
```

---

## 核心模块

### myRPC 框架

**协议格式**

```
请求：[4B method_name_len][4B args_len][method_name][args_bytes]
响应：[4B resp_len][resp_bytes]
```

当服务端连接数达上限（默认 1000）时，响应 `0xFFFFFFFF`（`kServerBusyMagic`），客户端可识别过载并返回 `err_code=1`。

**关键设计**

| 组件 | 实现 |
|------|------|
| 事件模型 | `epoll` ET 边缘触发 + `EPOLLONESHOT`，防止多线程并发处理同一 fd |
| IO 模型 | fd 全程非阻塞，`EAGAIN` 时调用 `yield()` 切换协程 |
| 并发模型 | 每个 worker 线程批量取任务，加入同一 `Scheduler` 协程调度，完成后 `clear()` 回收栈内存 |
| 方法分发 | Protobuf 反射（`ServiceDescriptor` + `CallMethod`），无需手写分发表 |
| 连接复用 | `ConnectionPool` 维护每端点空闲 fd 队列，`MSG_PEEK` 惰性健康检测 |
| 回调生命周期 | `ClosureGuard` RAII 确保 `done->Run()` 仅调用一次 |

**`call()` 错误码**

```cpp
int err_code = 0;
client.call(ip, port, method, args, &err_code);
// err_code: 0=OK  1=OVERLOAD（服务端连接数达上限）  2=NETWORK（网络错误）
```

---

### Raft 共识协议

完整实现 Raft 论文（Ongaro & Ousterhout, 2014）核心机制：

**领导选举**
- 随机选举超时：600–1000ms（`mt19937` 以 `node_id ⊕ 时间戳` 为种子，各节点独立）
- 冷启动保护：节点启动后 1500ms 内不触发选举
- 选举 RPC 期间**释放全局锁**，避免阻塞心跳和日志复制
- 客户端请求在选举期间**指数退避重试**（100ms → 1000ms，最多 5 次）

**日志复制**
- `AppendEntries` 一致性检查（`prevLogIndex` + `prevLogTerm`）
- 冲突截断 + 增量追加
- 多数派提交（`log_commit_index_` 推进），`condition_variable` 通知写请求
- 每条日志追加均调用 `fdatasync`，保证 crash-safe

**快照机制**
- 触发阈值：已应用日志数超过 1000 条（`kSnapshotThreshold`）
- 快照文件格式：`[Magic][Version][snap_index][snap_term][data_len][SnapshotData][CRC32]`
- 原子写：先写 `.tmp` 再 `rename`，防止部分写导致快照损坏
- CRC32 完整性校验（zlib）
- 落后 Follower 走 `InstallSnapshot` RPC

**协议细节修复**
- Candidate 收到同 term 的 `AppendEntries` 时正确降级为 Follower（Raft §5.2）
- `RequestVote` 包含日志完整性检查（Raft §5.4.1）

**持久化字段**
```
raft_state_<id>.txt   — current_term, voted_for, log_commit_index
raft_log_<id>.bin     — 二进制日志条目（protobuf 序列化）
raft_snapshot_<id>.bin — 快照文件（带 CRC32）
```

---

### 分布式 KV 存储

支持操作：

| 命令 | 说明 |
|------|------|
| `PUT key value` | 写入键值（走 Raft 日志复制，多数派提交后返回）|
| `DELETE key` | 删除键（同上）|
| `GET key` | 读取（Leader 本地读，无需日志复制）|
| `LIST prefix` | 前缀匹配返回所有键值对 |

`RaftClient` 自动处理 Leader 重定向（最多 10 次），非 Leader 节点响应中携带 Leader 地址。

---

### 协程调度器

基于 `ucontext`，每个协程独立 64KB 栈：

```
ThreadPool::worker_loop()
  └── 批量取出所有 pending 任务
  └── scheduler.add_coroutine(task) × N   // 全部加入同一 Scheduler
  └── scheduler.run()                      // 协程轮转执行，EAGAIN 时 yield()
  └── scheduler.clear()                    // 释放所有 FINISHED 协程栈内存
```

`yield()` 语义：当前协程让出 CPU，重新入队，调度器继续运行下一个就绪协程。在 IO 等待（`EAGAIN`）场景下实现无阻塞协作式并发。

---

### 连接池

`ConnectionPool`（Meyers Singleton）管理到各端点的 TCP 连接：

- 每端点默认最多 10 个连接（`max_per_endpoint_`）
- `acquire()`：优先复用空闲连接，`MSG_PEEK` 惰性健康检测，失败则新建
- `release()`：请求成功后归还 fd
- `discard()`：请求失败时 `close(fd)` 并归还名额
- 连接超时：建立 3s，读写 5s（`SO_RCVTIMEO` / `SO_SNDTIMEO`）

---

### 异步日志

| 特性 | 实现 |
|------|------|
| 写入路径 | `write_log()` → thread_local 缓冲 → 全局 `block_queue` → 异步线程写文件 |
| 文件 IO | `open()` + `write()`，直接进内核 page cache，`SIGKILL` 安全 |
| 日志轮转 | 按日期或行数（默认 500 万行）自动切分 |
| SIGTERM 处理 | 刷出 thread_local 缓冲，等待写线程完成，安全退出 |
| 4 个级别 | `DEBUG` / `INFO` / `WARN` / `ERROR` |
| 时间精度 | 微秒级时间戳 |

---

## 目录结构

```
.
├── include/
│   ├── mini_coroutine.h      # 协程调度器（Scheduler / Coroutine / yield）
│   ├── my_rpc.h              # RpcServer / RpcClient 声明
│   ├── my_log.h              # 异步日志宏与类声明
│   ├── blockQueue.h          # 线程安全队列 + thread_local 缓冲
│   ├── thread_pool.h         # 线程池声明
│   ├── connection_pool.h     # TCP 连接池
│   ├── metrics_manager.h     # RPC 调用计数（原子变量）
│   ├── closure_guard.h       # ClosureGuard RAII
│   ├── raft_node.h           # RaftNode 类声明
│   ├── raft_service_impl.h   # Protobuf RPC 服务实现声明
│   ├── raft_client.h         # 带退避重试的 Raft KV 客户端
│   ├── raft.proto            # Raft RPC 消息定义
│   ├── raft.pb.h / raft.pb.cc# Protobuf 生成代码
│   ├── service_registry.h    # 服务注册与发现
│   └── serviceImpl.h         # 示例 CalculatorService 实现
├── src/
│   ├── mini_coroutine.cpp    # 协程调度器实现
│   ├── thread_pool.cpp       # 线程池（批量协程调度）
│   ├── my_rpc.cpp            # RPC 框架实现（epoll 事件循环）
│   ├── my_log.cpp            # 异步日志实现
│   ├── connection_pool.cpp   # 连接池实现
│   ├── raft_node.cpp         # Raft 核心逻辑
│   ├── raft_service_impl.cpp # RPC 方法分发到 RaftNode
│   ├── raft.pb.cc            # Protobuf 生成代码
│   ├── service_registry.cpp  # 服务注册实现
│   ├── status_http_server.cpp# HTTP 指标暴露（端口 8080）
│   └── main.cpp              # RPC 框架示例入口
└── test/
    ├── raft_node_main.cpp    # Raft 节点启动入口
    ├── raft_test.cpp         # Raft KV 功能测试
    ├── pressure_test.cpp     # RPC 并发压力测试（512 线程）
    └── service_discovery_test.cpp # 服务发现测试
```

---

## 依赖

| 依赖 | 版本要求 | 用途 |
|------|----------|------|
| CMake | ≥ 3.10 | 构建系统 |
| GCC / Clang | 支持 C++17 | 编译器 |
| protobuf | 3.x | RPC 序列化 |
| zlib | 任意 | 快照 CRC32 校验 |
| pthread | POSIX | 线程支持 |

Ubuntu / Debian 安装：

```bash
sudo apt install cmake g++ libprotobuf-dev protobuf-compiler zlib1g-dev
```

---

## 构建与运行

### 构建

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j$(nproc)
```

构建产物：

| 可执行文件 | 说明 |
|-----------|------|
| `build/main` | RPC 框架演示（CalculatorService 客户端+服务端）|
| `build/raft_node_main` | Raft 节点进程 |
| `build/raft_test` | Raft KV 功能测试客户端 |
| `build/pressure_test` | RPC 并发压力测试 |
| `build/service_discovery_test` | 服务发现测试 |

### 启动 Raft 集群（3 节点示例）

```bash
# 节点 0：id=0, ip=127.0.0.1, port=8000，peer 列表用 id:ip:port 格式
./build/raft_node_main 0 127.0.0.1 8000 1:127.0.0.1:8001 2:127.0.0.1:8002 &

# 节点 1
./build/raft_node_main 1 127.0.0.1 8001 0:127.0.0.1:8000 2:127.0.0.1:8002 &

# 节点 2
./build/raft_node_main 2 127.0.0.1 8002 0:127.0.0.1:8000 1:127.0.0.1:8001 &
```

### 运行 KV 测试

```bash
# 向集群写入 a-z 共 26 个键值
./build/raft_test
```

### RPC 框架演示

```bash
# 启动 CalculatorService 服务端+客户端
./build/main
```

---

## 性能测试

`pressure_test` 启动 512 个并发线程，对本地 RPC 服务持续压测 10 秒并输出 QPS：

```bash
./build/pressure_test
# 输出示例：
# Total Time: 10012 ms
# Success: 382941
# Fail: 0
# QPS: 38247.1
```

测试覆盖：连接池复用、协程批量调度、非阻塞 IO、EPOLLONESHOT 重注册等完整链路。
