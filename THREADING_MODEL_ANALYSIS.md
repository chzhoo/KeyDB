# KeyDB 多线程模型框架深度分析

## 1. 总体架构概述

KeyDB 是 Redis 的高性能多线程分叉版本，其核心设计理念是 **"对称多线程"（Symmetric Multi-Threading）** —— 每个工作线程都是完全对等的，都拥有自己的事件循环（Event Loop），并且都能独立处理客户端连接的读写和命令执行。这与 Redis 6 的 `io-threads` 模型有本质区别：Redis 6 仅将 I/O 读写分配给辅助线程，命令执行仍然是单线程的；而 KeyDB 的工作线程可以同时执行命令。

### 线程模型总览

```
┌─────────────────────────────────────────────────────────────────┐
│                       KeyDB 进程                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐            │
│  │ Worker Thread │  │ Worker Thread │  │ Worker Thread │  ...     │
│  │     #0        │  │     #1        │  │     #2        │          │
│  │  (主线程)     │  │  (工作线程)    │  │  (工作线程)    │          │
│  ├──────────────┤  ├──────────────┤  ├──────────────┤            │
│  │ aeEventLoop  │  │ aeEventLoop  │  │ aeEventLoop  │            │
│  │ (epoll)      │  │ (epoll)      │  │ (epoll)      │            │
│  │              │  │              │  │              │            │
│  │ • TCP listen │  │ • TCP listen │  │ • TCP listen │            │
│  │ • Clients    │  │ • Clients    │  │ • Clients    │            │
│  │ • Timers     │  │ • Timers     │  │ • Timers     │            │
│  │ • Pipe (IPC) │  │ • Pipe (IPC) │  │ • Pipe (IPC) │            │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘            │
│         │                 │                 │                    │
│         └─────────────────┴─────────────────┘                    │
│                           │                                      │
│                    ┌──────┴──────┐                                │
│                    │ Global Lock │ ← 全局票据锁 (fastlock)        │
│                    │  (g_lock)   │                                │
│                    └──────┬──────┘                                │
│                           │                                      │
│                    ┌──────┴──────┐                                │
│                    │ 共享数据区   │                                │
│                    │ • DB 数据   │                                │
│                    │ • 客户端列表 │                                │
│                    │ • AOF/RDB   │                                │
│                    └─────────────┘                                │
│                                                                   │
│  ┌─────────────────────┐  ┌────────────┐  ┌────────────┐        │
│  │ AsyncWorkQueue      │  │ BIO Thread │  │ Time Thread│        │
│  │ (异步写线程池)       │  │ (后台 I/O) │  │ (时间更新)  │        │
│  └─────────────────────┘  └────────────┘  └────────────┘        │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## 2. 线程类型详解

### 2.1 主线程（Main/Startup Thread）

**文件**: `src/server.cpp` → `main()` 函数（约第 7785 行）

主线程负责完成以下初始化工作后，创建所有工作线程并等待它们退出：

```c
// server.cpp:7797-7826
for (int iel = 0; iel < cserver.cthreads; ++iel)
{
    pthread_create(g_pserver->rgthread + iel, &tattr, workerThreadMain, (void*)((int64_t)iel));
}

// 主线程在此阻塞等待所有工作线程退出
for (int iel = 0; iel < cserver.cthreads; ++iel)
    pthread_join(g_pserver->rgthread[iel], &pvRet);
```

### 2.2 工作线程（Worker Threads）

**文件**: `src/server.cpp` → `workerThreadMain()` （第 7365 行）

每个工作线程是完全对等的，线程编号通过 `iel` 标识（Index of Event Loop），范围 `[0, cserver.cthreads)`。工作线程 #0 即 `IDX_EVENT_LOOP_MAIN`，同时承担一些额外的主线程职责（如 `serverCron` 完整版）。

**配置项**: `server-threads` 或 `io-threads`（两者等价），最大值 `MAX_EVENT_LOOPS = 16`。

```c
// server.cpp:7365-7398
void *workerThreadMain(void *parg)
{
    int iel = (int)((int64_t)parg);
    serverTL = g_pserver->rgthreadvar + iel;  // 设置线程本地存储

    if (iel != IDX_EVENT_LOOP_MAIN)
    {
        aeThreadOnline();
        aeAcquireLock();
        initNetworkingThread(iel, cserver.cthreads > 1);  // 初始化网络监听
        aeReleaseLock();
        aeThreadOffline();
    }

    moduleAcquireGIL(true);
    aeThreadOnline();
    aeEventLoop *el = g_pserver->rgthreadvar[iel].el;
    aeMain(el);   // 进入事件循环主函数，永不返回（除非 shutdown）
    // ...
}
```

**关键数据结构 `redisServerThreadVars`**（每线程一份）:

```c
// server.h:2182-2216
struct redisServerThreadVars {
    aeEventLoop *el;                          // 该线程独有的事件循环
    socketFds ipfd;                           // TCP 监听 socket
    socketFds tlsfd;                          // TLS 监听 socket
    int in_eval;                              // 是否在 EVAL 中
    int in_exec;                              // 是否在 EXEC 中
    std::vector<client*> clients_pending_write; // 待写客户端列表
    list *unblocked_clients;                  // 待解除阻塞客户端
    list *clients_pending_asyncwrite;         // 异步写待处理
    int cclients;                             // 该线程上的客户端数量
    client *current_client;                   // 当前正在处理的客户端
    client *lua_client;                       // Lua 伪客户端
    struct fastlock lockPendingWrite;         // 待写锁
    const redisDbPersistentDataSnapshot **rgdbSnapshot; // MVCC 快照
    std::vector<client*> vecclientsProcess;   // 待处理客户端队列
    // ...
};
```

### 2.3 时间线程（Time Thread）

**文件**: `src/server.cpp` → `timeThreadMain()` （第 7331 行）

专用于高频更新 `server.unixtime` 和 `server.mstime` 等缓存时间值。使用极短的纳秒级休眠（100ns），确保时间精度：

```c
void *timeThreadMain(void*) {
    timespec delay;
    delay.tv_sec = 0;
    delay.tv_nsec = 100;
    while (true) {
        // 当所有工作线程都在 sleep 时，时间线程也休眠
        if (sleeping_threads >= cserver.cthreads) {
            time_thread_cv.wait(lock);
        }
        updateCachedTime();
        // nanosleep 100ns
    }
}
```

### 2.4 后台 I/O 线程（BIO Threads）

**文件**: `src/bio.cpp`

继承自 Redis 的 BIO 系统，共 3 个后台线程处理阻塞操作：

| 线程 | 类型 | 职责 |
|------|------|------|
| `bio_close_file` | `BIO_CLOSE_FILE` | 延迟关闭文件描述符 |
| `bio_aof_fsync` | `BIO_AOF_FSYNC` | AOF 文件 fsync |
| `bio_lazy_free` | `BIO_LAZY_FREE` | 延迟释放大对象内存 |

### 2.5 异步写线程池（AsyncWorkQueue）

**文件**: `src/AsyncWorkQueue.cpp`

用于处理异步写回复的线程池。线程数量等于 `cserver.cthreads`：

```c
// server.cpp:4151
g_pserver->asyncworkqueue = new AsyncWorkQueue(cserver.cthreads);
```

工作线程从队列中取出任务执行，完成后需要获取全局锁来刷新异步写缓冲区：

```c
void AsyncWorkQueue::WorkerThreadMain() {
    while (!m_fQuitting) {
        // 等待工作
        aeThreadOnline();
        while (!m_workqueue.empty()) {
            task.fnAsync();        // 执行异步任务（无锁）
        }
        // 刷新异步写
        if (listLength(serverTL->clients_pending_asyncwrite)) {
            aeAcquireLock();        // 获取全局锁
            ProcessPendingAsyncWrites();
            aeReleaseLock();
        }
        aeThreadOffline();
    }
}
```

## 3. 同步原语体系

### 3.1 Ticket Spinlock（fastlock）

**文件**: `src/fastlock.cpp`, `src/fastlock.h`

KeyDB 实现了一个自定义的 **票据自旋锁（Ticket Spinlock）**，这是整个多线程模型的基础同步原语。

**核心数据结构**:

```c
struct fastlock {
    volatile int m_pidOwner;     // 持有者线程 ID（可重入检测）
    volatile int m_depth;        // 重入深度
    char szName[56];             // 锁名称（调试用）
    // --- 独立缓存行 ---
    volatile struct ticket m_ticket;  // 票据计数器
    unsigned futex;              // Linux futex 位掩码
    char padding[56];            // 缓存行填充
};

struct ticket {
    uint16_t m_active;   // 当前活跃票据号（正在服务的号码）
    uint16_t m_avail;    // 下一个可分配的票据号
};
```

**工作原理**:

1. **加锁**: 原子递增 `m_avail` 获取一个票据，然后自旋等待 `m_active == myticket`
2. **解锁**: 原子递增 `m_active`，通过 Linux futex 唤醒下一个等待者
3. **公平性**: 票据锁保证 FIFO 顺序，避免饥饿
4. **可重入**: 通过 `m_pidOwner` 和 `m_depth` 支持递归加锁
5. **自适应**: 当 CPU 压力高时（通过 `sysinfo` 检测），减少自旋次数（从 `0x100000` 降到 `0x10000`），更早地进入 futex 休眠
6. **死锁检测**: `DeadlockDetector` 类通过维护线程→锁的等待图来检测死锁环

```c
// 加锁流程
void fastlock_lock(struct fastlock *lock, spin_worker worker) {
    // 1. 可重入检查
    if (lock->m_pidOwner == gettid()) { ++lock->m_depth; return; }

    // 2. 获取票据
    unsigned myticket = __atomic_fetch_add(&lock->m_ticket.m_avail, 1, ...);

    // 3. 自旋等待（可选执行有用工作）
    for (;;) {
        if ((lock->m_ticket.u & 0xffff) == myticket) break;
        __asm__ ("pause");  // x86 自旋提示
        if ((++cloops % loopLimit) == 0)
            fastlock_sleep(lock, tid, ...);  // futex 休眠
    }
}
```

### 3.2 全局锁（Global Lock）

**文件**: `src/ae.cpp`（第 89 行）

```c
fastlock g_lock("AE (global)");
```

这是 KeyDB 最核心的同步机制。**所有修改共享状态（数据库、客户端列表等）的操作都必须持有全局锁**。全局锁的语义类似于 Python 的 GIL，但 KeyDB 的创新在于：

1. **线程在 I/O 等待时释放全局锁**（epoll_wait 期间不持有锁）
2. **只读命令可以在不持有全局锁的情况下通过 MVCC 快照执行**
3. **网络读写操作标记为 `THREADSAFE` 的可以不持有全局锁执行**

### 3.3 Fork 读写锁（Fork Lock）

**文件**: `src/ae.cpp`（第 91 行）

```c
readWriteLock g_forkLock("Fork (global)");
```

这是一个读写锁，用于协调工作线程与 `fork()` 操作：

- **工作线程**: 持有读锁（`acquireRead`），多个线程可并发运行
- **fork 操作**: 需要升级为写锁（`upgradeWrite`），此时所有工作线程必须暂停

```c
void aeThreadOnline()  { g_forkLock.acquireRead(); }
void aeThreadOffline() { g_forkLock.releaseRead(); }
void aeAcquireForkLock() { g_forkLock.upgradeWrite(); }
```

### 3.4 客户端锁（Client Lock）

每个客户端都有一把独立的 `fastlock`：

```c
struct client {
    // ...
    struct fastlock lock {"client"};
    int iel;  // 该客户端绑定的事件循环索引
};
```

### 3.5 锁层次关系

锁获取的顺序必须严格遵循以下层次以避免死锁：

```
Fork Lock (读锁)     ← 最外层，所有线程在线时都持有
    └── Global Lock  ← 修改共享状态时获取
        └── Client Lock ← 操作特定客户端时获取
            └── lockPendingWrite ← 修改待写列表时获取
```

`AeLocker` 类（`src/aelocker.h`）封装了这个复杂的加锁序列，特别是处理了**先持有客户端锁再获取全局锁时避免死锁的逻辑**：

```c
void arm(client *c = nullptr) {
    if (c != nullptr) {
        // 持有客户端锁的情况下尝试获取全局锁
        if (!aeTryAcquireLock(true)) {
            // 无法立即获取，需要先释放客户端锁
            for (;;) {
                clientNesting = c->lock.unlock_recursive();  // 释放客户端锁
                aeAcquireLock();                              // 获取全局锁
                if (!c->lock.try_lock(false)) {              // 重新获取客户端锁
                    aeReleaseLock();                          // 失败则释放全局锁重试
                } else {
                    break;                                    // 两把锁都获取成功
                }
            }
        }
    }
}
```

## 4. 事件循环与线程协作

### 4.1 每线程事件循环

每个工作线程拥有独立的 `aeEventLoop`，使用 `epoll`（Linux）作为多路复用后端：

```c
// server.cpp:3880  initServerThread()
pvar->el = aeCreateEventLoop(g_pserver->maxclients + CONFIG_FDSET_INCR);
```

每个事件循环都有一对 pipe（`fdCmdRead`/`fdCmdWrite`）用于线程间通信：

```c
// ae.cpp:338-346
pipe(rgfd);
eventLoop->fdCmdRead = rgfd[0];
eventLoop->fdCmdWrite = rgfd[1];
aeCreateFileEvent(eventLoop, eventLoop->fdCmdRead, AE_READABLE|AE_READ_THREADSAFE,
                  aeProcessCmd, NULL);
```

### 4.2 跨线程投递机制（aePostFunction）

任何线程都可以通过 pipe 向指定事件循环投递任务：

```c
int aePostFunction(aeEventLoop *eventLoop, std::function<void()> fn, bool fLock, bool fForceQueue)
{
    if (eventLoop == g_eventLoopThisThread && !fForceQueue) {
        fn();        // 同线程直接执行
        return AE_OK;
    }
    // 跨线程通过 pipe 发送
    aeCommand cmd = {};
    cmd.op = AE_ASYNC_OP::PostCppFunction;
    cmd.pfn = new std::function<void()>(fn);
    write(eventLoop->fdCmdWrite, &cmd, sizeof(cmd));
}
```

**用途**：
- 将新连接迁移到目标线程
- 跨线程配置变更广播
- 异步关闭客户端

### 4.3 线程安全标记

事件的回调函数可以标记为线程安全，避免获取全局锁：

```c
#define AE_READ_THREADSAFE   8   // 可读事件无需全局锁
#define AE_WRITE_THREADSAFE  16  // 可写事件无需全局锁
#define AE_SLEEP_THREADSAFE  32  // before/afterSleep 无需全局锁
```

在 `ProcessEventCore()` 中：

```c
// ae.cpp:637-643
#define LOCK_IF_NECESSARY(fe, tsmask)
    std::unique_lock<decltype(g_lock)> ulock(g_lock, std::defer_lock);
    if (!(fe->mask & tsmask)) {       // 未标记线程安全
        g_forkLock.releaseRead();      // 释放 fork 读锁
        ulock.lock();                  // 获取全局锁
        g_forkLock.acquireRead();      // 重新获取 fork 读锁
    }
```

## 5. 连接分发与负载均衡

### 5.1 SO_REUSEPORT 多线程监听

当 `cserver.cthreads > 1` 时，每个工作线程都会使用 `SO_REUSEPORT` 创建独立的监听 socket：

```c
// server.cpp:3798-3813
static void initNetworkingThread(int iel, int fReusePort) {
    if (fReusePort || (iel == IDX_EVENT_LOOP_MAIN)) {
        listenToPort(g_pserver->port, &g_pserver->rgthreadvar[iel].ipfd, fReusePort, ...);
    }
}
```

这使得内核能够在多个线程之间分发新连接，避免惊群效应。

### 5.2 智能连接分配

`acceptOnThread()` 实现了智能负载均衡：

```c
// networking.cpp:1339-1387
void acceptOnThread(connection *conn, int flags, char *cip) {
    int ielTarget = ielCur;

    if (fBootLoad)
        ielTarget = IDX_EVENT_LOOP_MAIN;            // 启动加载期间只用主线程
    else if (g_pserver->active_client_balancing)
        ielTarget = chooseBestThreadForAccept();     // 选择负载最低的线程

    if (ielTarget != ielCur) {
        // 通过 aePostFunction 将连接迁移到目标线程
        aePostFunction(g_pserver->rgthreadvar[ielTarget].el, [conn, flags, ielTarget, szT] {
            connMarshalThread(conn);                 // 迁移连接到目标线程
            acceptCommonHandler(conn, flags, szT, ielTarget);
        });
    }
}
```

`chooseBestThreadForAccept()` 选择客户端数量最少的线程：

```c
int chooseBestThreadForAccept() {
    int ielMinLoad = 0;
    int cclientsMin = INT_MAX;
    for (int iel = 0; iel < cserver.cthreads; ++iel) {
        int cclientsThread = g_pserver->rgthreadvar[iel].cclients
                           + rgacceptsInFlight[iel]
                           + cclientsReplica * (replicaIsolationFactor - 1);
        if (cclientsThread < cclientsMin) {
            cclientsMin = cclientsThread;
            ielMinLoad = iel;
        }
    }
    return ielMinLoad;
}
```

## 6. 命令执行流水线（详细）

命令的完整生命周期从网络数据到达开始，经历**读取 → 解析 → 路由 → 执行 → 写回复**五个阶段。在多线程环境下，每个阶段对锁的要求截然不同，这正是 KeyDB 实现高吞吐量的关键。

### 6.1 总体流水线视图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         一次完整的命令生命周期                            │
│                                                                          │
│  ┌─ 阶段1: 网络读取 ──────────────────────────────────────────────┐      │
│  │  readQueryFromClient()                                          │      │
│  │  锁: 仅客户端锁(c->lock), 无全局锁                              │      │
│  │  工作: connRead() → 将数据追加到 c->querybuf                    │      │
│  └────────────────────────────┬─────────────────────────────────────┘      │
│                               ▼                                          │
│  ┌─ 阶段2: 协议解析 ──────────────────────────────────────────────┐      │
│  │  parseClientCommandBuffer()                                     │      │
│  │  锁: 仅客户端锁, 无全局锁                                       │      │
│  │  工作: RESP/Inline 协议解析 → 命令入队 c->vecqueuedcmd          │      │
│  │  附加: prefetchKeysAsync() 预取键值（无锁优化）                  │      │
│  └────────────────────────────┬─────────────────────────────────────┘      │
│                               ▼                                          │
│  ┌─ 阶段3: 命令路由（三条路径选择）─────────────────────────────────┐     │
│  │                                                                   │     │
│  │  路径A: 异步快速路径 (ASYNC)           ← 只读命令, 无全局锁       │     │
│  │  路径B: 延迟批量路径 (DEFERRED)        ← 写命令, 等待 beforeSleep │     │
│  │  路径C: 同步直通路径 (SYNC)            ← 单线程模式直接执行       │     │
│  │                                                                   │     │
│  └───────┬───────────────┬───────────────┬───────────────────────────┘     │
│          ▼               ▼               ▼                                │
│  ┌─ 阶段4: 命令执行 ──────────────────────────────────────────────┐      │
│  │  processCommand() → call() → cmd->proc()                       │      │
│  │  锁: 路径A=无全局锁+MVCC快照; 路径B/C=全局锁                    │      │
│  │  工作: 参数校验 → ACL检查 → 内存检查 → 执行 → 传播              │      │
│  └────────────────────────────┬─────────────────────────────────────┘      │
│                               ▼                                          │
│  ┌─ 阶段5: 写回复 ────────────────────────────────────────────────┐      │
│  │  路径A: addReply → replyAsync 缓冲区 → ProcessPendingAsyncWrites│      │
│  │  路径B/C: addReply → c->buf/c->reply → handlePendingWrites     │      │
│  │  最终: writeToClient() → connWrite() 发送到 socket              │      │
│  └──────────────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

### 6.2 阶段1：网络读取 `readQueryFromClient()`

**源码位置**: `networking.cpp:2684`

当 epoll 报告某个客户端 socket 可读时，该客户端所绑定的工作线程调用此函数。**关键点：此阶段不需要全局锁**，因为网络读取事件被标记为 `AE_READ_THREADSAFE`。

```c
void readQueryFromClient(connection *conn) {
    client *c = (client*)connGetPrivateData(conn);

    // ① 断言：不持有全局锁
    serverAssertDebug(!GlobalLocksAcquired());

    // ② 尝试获取客户端锁（非阻塞）
    std::unique_lock<decltype(c->lock)> lock(c->lock, std::defer_lock);
    if (!lock.try_lock())
        return;  // 锁被占用，跳过此次，下次事件循环再处理

    // ③ 从 socket 读取数据到 querybuf
    nread = connRead(c->conn, c->querybuf + qblen, readlen);

    // ④ 后续处理...（见阶段2-3）
}
```

**锁状态**: 仅持有 `c->lock`（客户端锁），使用 `try_lock` 非阻塞获取。如果获取失败（比如异步写线程正在操作该客户端），直接返回，避免阻塞事件循环。

**线程安全保证**: 每个客户端绑定到固定的事件循环（`c->iel`），读取操作只在该线程上发生，因此不存在多个线程同时读同一个客户端的情况。

---

### 6.3 阶段2：协议解析 `parseClientCommandBuffer()`

**源码位置**: `networking.cpp:2572`

此阶段将 `c->querybuf` 中的原始字节流解析为结构化命令，存入 `c->vecqueuedcmd` 队列：

```c
void parseClientCommandBuffer(client *c) {
    while (c->qb_pos < sdslen(c->querybuf)) {
        // ① 判断协议类型
        if (c->querybuf[c->qb_pos] == '*')
            c->reqtype = PROTO_REQ_MULTIBULK;   // RESP 协议
        else
            c->reqtype = PROTO_REQ_INLINE;       // 内联协议

        // ② 解析命令，结果入队 c->vecqueuedcmd
        if (c->reqtype == PROTO_REQ_INLINE)
            processInlineBuffer(c);
        else
            processMultibulkBuffer(c);

        // ③ 异步键值预取（减少后续执行时的缓存未命中）
        if (g_pserver->prefetch_enabled && !GlobalLocksAcquired()) {
            c->db->prefetchKeysAsync(c, query);
        }
    }
}
```

**`vecqueuedcmd` 的角色**: 这是每个客户端的命令队列。解析后的命令（`parsed_command`）存在这里，等待执行。一次 `read()` 可能读到多个完整命令（管道化 pipelining），这些命令都被解析后排队。

**键值预取**: 一个重要的性能优化。在不持有全局锁时，提前将键值从存储层预取到 CPU 缓存中（`prefetchKeysAsync`），这样后续执行命令时就不会因为缓存未命中而阻塞。

---

### 6.4 阶段3：命令路由（三条执行路径）

**源码位置**: `networking.cpp:2762-2784`

这是整个流水线最关键的分叉点。解析完成后，KeyDB 需要决定通过哪条路径执行命令：

```c
// readQueryFromClient() 的后半部分
if (cserver.cthreads > 1 || g_pserver->m_pstorageFactory) {
    // ==================== 多线程模式 ====================
    parseClientCommandBuffer(c);

    // ─── 路径A：异步快速路径（无全局锁） ───
    if (g_pserver->enable_async_commands           // 功能启用
        && !serverTL->disable_async_commands        // 本轮未被禁用
        && listLength(g_pserver->monitors) == 0     // 无 MONITOR 客户端
        && (aeLockContention()                      // 全局锁有竞争
            || serverTL->rgdbSnapshot[c->db->id]    // 或已有快照
            || g_fTestMode)                         // 或测试模式
        && !serverTL->in_eval                       // 不在 EVAL 中
        && !serverTL->in_exec)                      // 不在 EXEC 中
    {
        // 频繁写入的客户端不适合此优化（避免频繁更新快照）
        bool fSnapshotExists = c->db->mvccLastSnapshot >= c->mvccCheckpoint;
        bool fWriteTooRecent = (getMvccTstamp() - c->mvccCheckpoint) 太小;

        if (!fWriteTooRecent || fSnapshotExists) {
            processInputBuffer(c, false, CMD_CALL_ASYNC);
            // 此调用只执行标记为 CMD_ASYNC_OK | CMD_READONLY 的命令
            // 其余命令留在 vecqueuedcmd 中
        }
    }

    // ─── 路径B：延迟批量路径 ───
    if (!c->vecqueuedcmd.empty())
        serverTL->vecclientsProcess.push_back(c);
        // 推入待处理列表，等 beforeSleep() 批量执行

} else {
    // ==================== 单线程模式 ====================
    // ─── 路径C：同步直通路径 ───
    AeLocker locker;
    locker.arm(c);  // 获取全局锁
    runAndPropogateToReplicas(processInputBuffer, c, true, CMD_CALL_FULL);
}
```

#### 路径A的条件判定（`FAsyncCommand` 函数）

```c
bool FAsyncCommand(parsed_command &cmd) {
    if (serverTL->in_eval || serverTL->in_exec)
        return false;
    auto parsedcmd = lookupCommand(szFromObj(cmd.argv[0]));
    if (parsedcmd == nullptr)
        return false;
    static const long long expectedFlags = CMD_ASYNC_OK | CMD_READONLY;
    return (parsedcmd->flags & expectedFlags) == expectedFlags;
}
```

只有同时带有 `CMD_ASYNC_OK`（命令实现声明自己支持异步）和 `CMD_READONLY`（只读命令）两个标志的命令才能走路径A。典型命令：`GET`、`MGET`、`STRLEN`、`EXISTS`、`TTL` 等。

#### 路径A的智能开关机制

路径A并不是无条件启用的，它有一套精密的开关逻辑：

| 条件 | 目的 |
|------|------|
| `aeLockContention()` 为真 | 只在锁有竞争时才启用（无竞争时获取全局锁更快） |
| `c->mvccCheckpoint` 检查 | 频繁写入的客户端跳过（写入会导致快照频繁失效） |
| `snapshot_slip` 阈值 | 允许快照滞后的最大时间（默认 500ms） |
| `disable_async_commands` | 当快照创建失败时关闭本轮异步命令 |

---

### 6.5 阶段4a：异步命令执行（路径A详解）

这是 KeyDB 最大的性能创新。命令无需全局锁，直接通过 MVCC 快照执行。

#### 进入 `processInputBuffer()` 的异步模式

```c
void processInputBuffer(client *c, bool fParse, int callFlags) {
    while (!c->vecqueuedcmd.empty()) {
        auto &cmd = c->vecqueuedcmd.front();

        // 关键检查：异步模式下，遇到非异步命令立即停止
        if ((callFlags & CMD_CALL_ASYNC) && !FAsyncCommand(cmd))
            break;

        // 将 parsed_command 的 argv 转移给客户端
        c->argc = cmd.argc;
        c->argv = cmd.argv;
        cmd.argv = nullptr;
        c->vecqueuedcmd.erase(c->vecqueuedcmd.begin());

        // 执行命令
        processCommandAndResetClient(c, callFlags);
    }
}
```

#### `processCommand()` 的异步路径

```c
int processCommand(client *c, int callFlags) {
    // 断言：要么持有全局锁，要么是异步命令
    serverAssert((callFlags & CMD_CALL_ASYNC) || GlobalLocksAcquired());

    // 异步模式下跳过内存驱逐（需要全局锁）
    if (g_pserver->maxmemory && !(callFlags & CMD_CALL_ASYNC)) {
        performEvictions(false);
    }

    // ... 各种前置检查（ACL、集群重定向、只读副本等）...

    // 最终调用 call()
    call(c, callFlags);
}
```

#### `call()` 中的异步特殊处理

```c
void call(client *c, int flags) {
    // 断言：异步模式下命令必须是只读的
    serverAssert(((flags & CMD_CALL_ASYNC) && (c->cmd->flags & CMD_READONLY))
                 || GlobalLocksAcquired());

    // 异步模式下不初始化 also_propagate（无需传播）
    if (!(flags & CMD_CALL_ASYNC)) {
        prev_also_propagate = g_pserver->also_propagate;
        redisOpArrayInit(&g_pserver->also_propagate);
    }

    incrementMvccTstamp();  // 递增 MVCC 时间戳

    // ★ 实际执行命令处理函数
    c->cmd->proc(c);

    // 异步模式下 dirty 设为 0（无需同步）
    if (flags & CMD_CALL_ASYNC)
        dirty = 0;

    // 异步模式下不传播到 AOF/副本（因为是只读操作）
}
```

#### MVCC 快照如何服务读请求

当异步的只读命令（如 `GET key`）执行时，最终会调用 `lookupKeyRead()` 的异步重载版本：

```c
robj_roptr lookupKeyRead(redisDb *db, robj *key, uint64_t mvccCheckpoint, AeLocker &locker) {
    robj_roptr o;

    if (aeThreadOwnsLock()) {
        // 持有全局锁 → 走正常路径
        return lookupKeyReadWithFlags(db, key, LOOKUP_NONE);
    } else {
        // ★ 这是异步命令的核心路径
        if (keyIsExpired(db, key))
            return nullptr;

        int idb = db->id;

        // 检查是否需要创建/更新快照
        if (serverTL->rgdbSnapshot[idb] == nullptr
            || serverTL->rgdbSnapshot[idb]->mvccCheckpoint() < mvccCheckpoint)
        {
            // 需要获取全局锁来创建快照
            locker.arm(serverTL->current_client);

            if (serverTL->rgdbSnapshot[idb] != nullptr) {
                // 快照过旧，需要结束旧快照
                db->endSnapshot(serverTL->rgdbSnapshot[idb]);
                serverTL->rgdbSnapshot[idb] = nullptr;
            } else {
                // 创建新快照
                serverTL->rgdbSnapshot[idb] = db->createSnapshot(mvccCheckpoint, true);
            }

            if (serverTL->rgdbSnapshot[idb] == nullptr) {
                // 快照创建失败（可选的快照，fOptional=true）
                o = lookupKeyReadWithFlags(db, key, LOOKUP_NONE);
                serverTL->disable_async_commands = true;  // 禁用后续异步命令
            } else {
                locker.disarm();  // ★ 快照创建成功后释放全局锁！
            }
        }

        // ★ 在快照上执行线程安全的键查找
        if (serverTL->rgdbSnapshot[idb] != nullptr) {
            o = serverTL->rgdbSnapshot[idb]->find_cached_threadsafe(szFromObj(key)).val();
        }
    }
    return o;
}
```

**快照生命周期**:
1. 首次异步命令 → 获取全局锁 → `createSnapshot()` → 释放全局锁
2. 后续异步命令 → 直接使用已有快照（**完全无锁**）
3. `beforeSleep()` → 检查快照是否过期（`FStale()`）→ `endSnapshot()`

**快照的关键优势**: 创建快照只需持有一次全局锁，之后该线程上的所有只读命令都可以复用这个快照，在无锁的情况下执行。这意味着在全局锁被其他线程持有时，本线程仍然可以高速处理只读请求。

---

### 6.6 阶段4b：延迟批量执行（路径B详解）

不满足异步条件的命令（写命令、不支持异步的读命令等）会留在 `c->vecqueuedcmd` 中，客户端指针被推入 `serverTL->vecclientsProcess`。

这些命令在 `beforeSleep()` → `processClients()` 中被批量执行：

```c
// beforeSleep() 中：
locker.arm();  // 获取全局锁
runAndPropogateToReplicas(processClients);

// processClients()：
void processClients() {
    serverAssert(GlobalLocksAcquired());  // 必须持有全局锁

    while (!serverTL->vecclientsProcess.empty()) {
        client *c = serverTL->vecclientsProcess.front();
        serverTL->vecclientsProcess.erase(serverTL->vecclientsProcess.begin());

        std::unique_lock<fastlock> ul(c->lock);  // 获取客户端锁
        processInputBuffer(c, false, CMD_CALL_FULL);  // CMD_CALL_FULL 包含传播
    }

    // 处理异步写
    if (listLength(serverTL->clients_pending_asyncwrite))
        ProcessPendingAsyncWrites();
}
```

**批量执行的好处**:
1. 全局锁只获取一次，然后连续处理多个客户端的命令
2. 减少锁获取/释放的开销
3. `runAndPropogateToReplicas` 包裹器确保复制数据被批量刷新

#### `runAndPropogateToReplicas` 包裹器

```c
template<typename FN_PTR, typename... TARGS>
void runAndPropogateToReplicas(FN_PTR *pfn, TARGS... args) {
    bool fNestedProcess = (g_pserver->repl_batch_idxStart >= 0);
    if (!fNestedProcess) {
        // 记录复制偏移量起始位置
        g_pserver->repl_batch_offStart = g_pserver->master_repl_offset;
        g_pserver->repl_batch_idxStart = g_pserver->repl_backlog_idx;
    }

    pfn(args...);  // 执行实际函数

    if (!fNestedProcess) {
        // 将累积的复制数据一次性刷新给所有副本
        flushReplBacklogToClients();
        g_pserver->repl_batch_offStart = -1;
    }
}
```

---

### 6.7 阶段4c：同步直通执行（路径C详解）

单线程模式（`cserver.cthreads == 1`）下的最短路径：

```c
// readQueryFromClient() 中的单线程分支
AeLocker locker;
locker.arm(c);  // 获取全局锁
runAndPropogateToReplicas(processInputBuffer, c, true /*fParse*/, CMD_CALL_FULL);
```

这里 `processInputBuffer` 的 `fParse=true` 参数表示在执行前先调用 `parseClientCommandBuffer(c)` 重新解析（因为单线程模式下之前没有单独解析过）。

**为什么单线程模式不走路径B的延迟批量？**

```c
// 注释原文：If we're single threaded its actually better to just
// process the command here while the query is hot in the cache.
// Multithreaded lock contention dominates and batching is better.
```

单线程无锁竞争，数据在 CPU 缓存中是热的（刚从 querybuf 解析出来），直接执行效率最高。多线程下锁竞争是瓶颈，所以批量化更有利。

---

### 6.8 阶段5：写回复的两条路径

命令执行完成后需要将结果写回客户端。根据命令是否在正确的线程上执行，写回复分为**同步路径**和**异步路径**。

#### `addReply()` 的入口选择

```c
void addReply(client *c, robj_roptr obj) {
    if (prepareClientToWrite(c) != C_OK) return;
    _addReplyToBuffer(c, data, len);
}
```

`prepareClientToWrite()` 是路由的关键：

```c
int prepareClientToWrite(client *c) {
    bool fAsync = !FCorrectThread(c);  // 不在正确线程上 → 异步

    if (!fAsync) {
        // 同步路径：安装写处理器
        clientInstallWriteHandler(c);
    } else {
        // 异步路径：加入异步写列表
        clientInstallAsyncWriteHandler(c);
    }
    return C_OK;
}
```

#### 同步写路径

```c
void clientInstallWriteHandler(client *c) {
    if (!(c->flags & CLIENT_PENDING_WRITE)) {
        c->flags |= CLIENT_PENDING_WRITE;
        // 加入线程本地的待写列表
        std::unique_lock<fastlock> lockf(g_pserver->rgthreadvar[c->iel].lockPendingWrite);
        g_pserver->rgthreadvar[c->iel].clients_pending_write.push_back(c);
    }
}
```

数据写入 `c->buf`（小回复的固定缓冲区）或 `c->reply`（大回复的链表）。

#### 异步写路径

当命令在**非绑定线程**上执行时（如全局锁持有线程恰好不是客户端所属线程），回复数据写入特殊的 `c->replyAsync` 缓冲区：

```c
int _addReplyToBuffer(client *c, const char *s, size_t len) {
    bool fAsync = !FCorrectThread(c);
    if (fAsync) {
        // 写入异步缓冲区（不需要客户端锁，因为只有当前线程操作）
        if (c->replyAsync == nullptr) {
            c->replyAsync = (clientReplyBlock*)zmalloc(...);
        }
        memcpy(c->replyAsync->buf() + c->replyAsync->used, s, len);
        c->replyAsync->used += len;
    } else {
        // 写入正常缓冲区
        memcpy(c->buf + c->bufpos, s, len);
        c->bufpos += len;
    }
}
```

异步缓冲区稍后由 `ProcessPendingAsyncWrites()` 合并到正常缓冲区。

#### `handleClientsWithPendingWrites()` —— 写回复的最终执行

这是 `beforeSleep()` 中释放全局锁后调用的函数：

```c
int handleClientsWithPendingWrites(int iel, int aof_state) {
    // ① 先处理异步写缓冲区
    if (listLength(serverTL->clients_pending_asyncwrite)) {
        AeLocker locker;
        locker.arm(nullptr);
        ProcessPendingAsyncWrites();  // 合并 replyAsync → buf/reply
    }

    // ② 取出待写客户端列表
    std::unique_lock<fastlock> lockf(g_pserver->rgthreadvar[iel].lockPendingWrite);
    auto vec = std::move(g_pserver->rgthreadvar[iel].clients_pending_write);
    lockf.unlock();

    // ③ 尝试直接写入 socket（避免注册写事件的系统调用开销）
    for (client *c : vec) {
        if (writeToClient(c, 0) == C_ERR)
            continue;

        // ④ 如果还有数据未写完，注册写事件处理器
        if (clientHasPendingReplies(c)) {
            connSetWriteHandlerWithBarrier(c->conn, sendReplyToClient, ae_flags, true);
        }
    }
}
```

`writeToClient()` 尝试直接 `connWrite()` 发送数据。如果一次写不完（socket 缓冲区满），则注册 `sendReplyToClient` 作为写事件回调，等到下次 epoll 报告可写时继续发送。

**注意**: `writeToClient()` 和 `sendReplyToClient()` 都标记为 `AE_WRITE_THREADSAFE`，**不需要全局锁**，仅需要客户端锁。

---

### 6.9 `ProcessPendingAsyncWrites()` —— 异步写的合并

这个函数将 `replyAsync` 异步缓冲区的数据合并到客户端的正常输出缓冲区：

```c
void ProcessPendingAsyncWrites() {
    serverAssert(GlobalLocksAcquired());  // 需要全局锁

    while (listLength(serverTL->clients_pending_asyncwrite)) {
        client *c = listFirst(serverTL->clients_pending_asyncwrite);
        std::lock_guard<decltype(c->lock)> lock(c->lock);

        if (c->replyAsync != nullptr) {
            size_t size = c->replyAsync->used;

            if (listLength(c->reply) == 0 && size <= PROTO_REPLY_CHUNK_BYTES - c->bufpos) {
                // 小回复：直接追加到固定缓冲区
                memcpy(c->buf + c->bufpos, c->replyAsync->buf(), size);
                c->bufpos += size;
            } else {
                // 大回复：追加到回复链表
                listAddNodeTail(c->reply, c->replyAsync);
                c->replyAsync = nullptr;
            }
            zfree(c->replyAsync);
            c->replyAsync = nullptr;
        }

        // 通知客户端所属线程安装写事件
        if (FCorrectThread(c)) {
            prepareClientToWrite(c);  // 同线程直接安装
        } else {
            // 跨线程通过 postFunction 投递
            c->postFunction([](client *c) {
                clientInstallWriteHandler(c);
                handleClientsWithPendingWrites(c->iel, g_pserver->aof_state);
            }, false);
        }
    }
}
```

---

### 6.10 完整时序图：一个 GET 命令的多线程生命周期

以下展示了一个 `GET mykey` 命令在多线程高负载场景下走异步路径A的完整时序：

```
Thread #1 (客户端绑定线程)                 Thread #0 (持有全局锁处理写命令)
═══════════════════════════               ═══════════════════════════════
                                          持有 g_lock, 执行 SET/DEL 等

epoll_wait() 返回: client fd 可读
│
afterSleep()
├─ aeThreadOnline()                       │
│                                         │
readQueryFromClient()                     │ (g_lock 被 Thread #0 持有)
├─ c->lock.try_lock() ✓                  │
├─ connRead() → "GET mykey\r\n"          │
├─ parseClientCommandBuffer()             │
│  └─ vecqueuedcmd += {GET, mykey}        │
│                                         │
├─ [条件满足: CMD_ASYNC_OK,               │
│   aeLockContention()=true]              │
│                                         │
├─ processInputBuffer(ASYNC)              │
│  └─ processCommandAndResetClient(ASYNC) │
│     └─ processCommand(ASYNC)            │
│        └─ call(ASYNC)                   │
│           └─ getCommand(c)              │
│              └─ lookupKeyRead(ASYNC)    │
│                 ├─ rgdbSnapshot == NULL  │
│                 ├─ locker.arm(c)        │
│                 │  ├─ c->lock.unlock()  │
│                 │  ├─ aeAcquireLock()   │ ← 等待 Thread #0 释放
│                 │  │      ...等待...     │
│                 │  │                    aeReleaseLock() ← Thread #0 释放
│                 │  ├─ g_lock 获取 ✓     │
│                 │  └─ c->lock.lock() ✓  │
│                 │                       │
│                 ├─ createSnapshot() ✓   │
│                 ├─ locker.disarm()       │ ← ★ 释放全局锁！
│                 │  └─ aeReleaseLock()   │
│                 │                       │ Thread #0 可以再次获取锁
│                 └─ snapshot->find("mykey") ← 无锁读取！
│                    └─ 返回 "hello"      │
│                                         │
│           └─ addReply(c, "hello")       │
│              └─ _addReplyToBuffer(同线程)│
│                 └─ memcpy → c->buf      │
│                                         │
│           └─ commandProcessed()         │
│              └─ resetClient()           │
│                                         │
├─ c->lock.unlock()                       │
│                                         │
beforeSleep()                             │
├─ locker.arm() → g_lock                  │
├─ [检查快照是否过期]                     │
├─ handleClientsWithPendingWrites()       │
│  ├─ locker.disarm() → 释放 g_lock      │
│  ├─ writeToClient(c, 0)                │
│  │  └─ connWrite("$5\r\nhello\r\n")    │ ← 发送到 socket
│  └─ [写完, 无需注册写事件]              │
│                                         │
epoll_wait()                              │
```

**关键观察**:
1. 快照创建只需短暂持有全局锁（微秒级）
2. 数据查找通过快照完成，完全无锁
3. 后续相同 DB 的 GET 命令可以直接复用快照，全程无锁
4. `writeToClient()` 也无需全局锁

---

### 6.11 对比：一个 SET 命令走路径B的完整时序

```
Thread #1                                Thread #0
═════════                                ═════════

readQueryFromClient()
├─ connRead() → "SET mykey hello\r\n"
├─ parseClientCommandBuffer()
│  └─ vecqueuedcmd += {SET, mykey, hello}
│
├─ [尝试异步: FAsyncCommand() = false]   ← SET 不是 CMD_READONLY
│  (SET 有 CMD_WRITE 标志, 不满足条件)
│
├─ vecclientsProcess.push_back(c)        ← 推迟到 beforeSleep
│
beforeSleep()
├─ locker.arm() → g_lock                 (等待 g_lock)
├─ runAndPropogateToReplicas(processClients)
│  ├─ 记录 repl_batch_offStart
│  ├─ processClients()
│  │  ├─ c->lock.lock()
│  │  └─ processInputBuffer(CMD_CALL_FULL)
│  │     └─ processCommandAndResetClient(CMD_CALL_FULL)
│  │        └─ processCommand(CMD_CALL_FULL)
│  │           ├─ performEvictions()
│  │           └─ call(CMD_CALL_FULL)
│  │              ├─ setCommand(c)
│  │              │  └─ lookupKeyWrite() / dbAdd()
│  │              ├─ dirty++
│  │              ├─ c->mvccCheckpoint = getMvccTstamp()
│  │              └─ propagate() → AOF + 副本
│  │  └─ c->lock.unlock()
│  ├─ flushReplBacklogToClients()
│  └─ repl_batch_offStart = -1
│
├─ handleClientsWithPendingWrites()
│  ├─ locker.disarm() → 释放 g_lock
│  └─ writeToClient() → connWrite("+OK\r\n")
│
epoll_wait()
```

---

### 6.12 `commandProcessed()` —— 命令后处理

每个命令成功执行后调用，负责清理和复制偏移量更新：

```c
void commandProcessed(client *c, int flags) {
    if (c->flags & CLIENT_BLOCKED) return;

    resetClient(c);  // 清理 argv, 重置标志位

    // 如果客户端是主节点（复制场景），更新已应用的复制偏移量
    if (c->flags & CLIENT_MASTER && !(c->flags & CLIENT_MULTI)) {
        c->reploff = c->reploff_cmd;
    }

    // 将复制数据传播给下游副本
    if (c->flags & CLIENT_MASTER) {
        long long applied = c->reploff - prev_offset;
        if (applied) {
            replicationFeedSlavesFromMasterStream(applied);
        }
    }
}
```

---

### 6.13 性能影响汇总

| 阶段 | 持有全局锁 | 持有客户端锁 | 可并行度 |
|------|:----------:|:----------:|:--------:|
| 网络读取 | ✗ | ✓ | 完全并行 |
| 协议解析 | ✗ | ✓ | 完全并行 |
| 键值预取 | ✗ | ✓ | 完全并行 |
| 只读执行(快照) | 创建时短暂持有 | ✓ | 近乎完全并行 |
| 写命令执行 | ✓ | ✓ | 串行 |
| 写回复(buf) | ✗ | ✓ | 完全并行 |
| 异步回复合并 | ✓ | ✓ | 串行 |
| 传播到副本 | ✓ | ✗ | 串行 |

**核心结论**: 对于读多写少的工作负载，KeyDB 的多线程效果最为显著——所有读操作都能真正并行执行。对于写密集型工作负载，性能提升主要来自 I/O 读写的并行化。

## 7. 锁获取/释放的时序分析

以一次典型的事件循环迭代为例：

```
时间轴 →

Thread #0                    Thread #1                    Thread #2
─────────                    ─────────                    ─────────
epoll_wait() [无锁]          epoll_wait() [无锁]          epoll_wait() [无锁]
    │                            │                            │
afterSleep()                 afterSleep()                 afterSleep()
├─ forkLock.acquireRead()    ├─ forkLock.acquireRead()    ├─ forkLock.acquireRead()
├─ g_lock.lock() → 获取      │  (等待全局锁)              │  (等待全局锁)
├─ trackChanges()            │                            │
├─ g_lock.unlock()           │                            │
│                            ├─ g_lock.lock() → 获取      │  (等待)
│                            ├─ trackChanges()            │
│                            ├─ g_lock.unlock()           │
│                            │                            ├─ g_lock.lock() → 获取
│                            │                            ├─ trackChanges()
│                            │                            ├─ g_lock.unlock()
│                            │                            │
[处理已触发事件]              [处理已触发事件]              [处理已触发事件]
│                            │                            │
readQueryFromClient()        readQueryFromClient()        readQueryFromClient()
├─ c->lock.lock()            ├─ c->lock.lock()            ├─ c->lock.lock()
├─ connRead() [无全局锁]     ├─ connRead() [无全局锁]     ├─ connRead() [无全局锁]
├─ parseCommand()            ├─ parseCommand()            ├─ parseCommand()
│                            │                            │
├─ [只读?] 异步执行          ├─ 加入 vecclientsProcess    ├─ 异步执行
│  processCmd(ASYNC)         │                            │  processCmd(ASYNC)
│  使用 MVCC 快照            │                            │  使用 MVCC 快照
│                            │                            │
├─ c->lock.unlock()          ├─ c->lock.unlock()          ├─ c->lock.unlock()
│                            │                            │
beforeSleep()                beforeSleep()                beforeSleep()
├─ g_lock.lock() → 获取      │  (等待)                    │  (等待)
├─ processClients()          │                            │
│  └─ processInputBuffer()   │                            │
├─ handlePendingWrites()     │                            │
├─ g_lock.unlock()           │                            │
│                            ├─ g_lock.lock() → 获取      │  (等待)
│                            ├─ processClients()          │
│                            ├─ handlePendingWrites()     │
│                            ├─ g_lock.unlock()           │
│                            │                            ├─ g_lock.lock()
│                            │                            ├─ ...
epoll_wait() [释放锁]       epoll_wait() [释放锁]       epoll_wait() [释放锁]
```

## 8. 关键设计决策与权衡

### 8.1 全局锁 vs 细粒度锁

KeyDB 选择了 **单一全局锁 + MVCC 快照** 而非细粒度锁，原因是：

| 方面 | 全局锁 + MVCC | 细粒度锁 |
|------|--------------|---------|
| **复杂度** | 低：原有 Redis 代码改动小 | 高：需要重构所有数据结构 |
| **原子性** | 天然保证事务原子性 | 需要复杂的两阶段锁 |
| **吞吐量** | 通过 I/O 并行和快照读获得提升 | 理论上更好，但锁开销大 |
| **兼容性** | 完全兼容 Redis 语义 | 可能引入语义差异 |

### 8.2 Ticket Lock vs std::mutex

KeyDB 使用自定义的 Ticket Lock 而非 `std::mutex`：

- **公平性**: Ticket Lock 保证 FIFO，避免线程饥饿
- **可重入**: 内建可重入支持
- **自适应**: 根据 CPU 负载动态调整自旋次数
- **Futex 整合**: 利用 Linux futex 实现高效休眠/唤醒
- **缓存友好**: 通过 padding 确保 `m_ticket` 在独立缓存行上

### 8.3 每线程独立事件循环 vs 共享事件循环

每个线程有独立的 `aeEventLoop` 和 `epoll` 实例：

- **优点**: 减少锁竞争；`SO_REUSEPORT` 让内核分发连接；每线程独立阻塞在 epoll_wait
- **代价**: 需要 pipe 机制进行线程间通信；需要连接迁移逻辑

## 9. 线程间通信机制总结

| 机制 | 方向 | 场景 | 锁需求 |
|------|------|------|--------|
| `aePostFunction` (pipe) | 任意 → 目标线程 | 连接迁移、配置变更 | 写入端无需锁 |
| `vecclientsProcess` | 读线程 → beforeSleep | 待处理命令批量提交 | 线程本地 |
| `clients_pending_write` | beforeSleep → 写事件 | 待写客户端列表 | `lockPendingWrite` |
| `AsyncWorkQueue` | 主线程 → 写线程池 | 异步写回复 | `std::mutex` |
| `g_lock` (全局锁) | 互斥 | 所有数据修改 | `fastlock` |
| `g_forkLock` | 协作 | fork 操作 | `readWriteLock` |
| `time_thread_cv` | 工作线程 → 时间线程 | 唤醒时间更新 | `fastlock` |

## 10. 性能关键路径分析

### 无锁快速路径（Fast Path）

对于只读命令（如 GET、MGET 等），KeyDB 可以实现近乎无锁执行：

1. `readQueryFromClient`: 只需客户端锁，标记为 `AE_READ_THREADSAFE`
2. `parseClientCommandBuffer`: 无需全局锁
3. `processInputBuffer(CMD_CALL_ASYNC)`: 使用 MVCC 快照，无需全局锁
4. 写回复到 `replyAsync` 缓冲区: 无需全局锁
5. 最终由 `AsyncWorkQueue` 或 `beforeSleep` 刷新到客户端

### 全局锁热点路径（Hot Path）

以下操作需要全局锁，是潜在的性能瓶颈：

1. **写命令执行**: SET、DEL 等修改数据的命令
2. **beforeSleep 处理**: `processClients()`、`handlePendingWrites()`
3. **afterSleep 初始化**: `trackChanges()`
4. **客户端创建/销毁**: `acceptCommonHandler()`、`freeClient()`
5. **复制相关**: AOF 写入、复制传播

## 11. 配置与调优

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `server-threads` | 1 | 工作线程数，推荐 ≤ 4 |
| `server-thread-affinity` | false | 是否绑定 CPU 核心 |
| `active-client-balancing` | yes | 是否启用智能连接分配 |
| `min-clients-per-thread` | - | 线程最低客户端阈值 |
| `enable-async-commands` | - | 是否启用 MVCC 异步只读命令 |

## 总结

KeyDB 的多线程模型是一个务实的工程折中方案：

1. **保持 Redis 兼容性**: 通过全局锁保证原子性语义不变
2. **I/O 并行化**: 每个线程独立处理 I/O，读写网络数据不需要全局锁
3. **读操作加速**: 通过 MVCC 快照允许只读命令无锁并行执行
4. **最小化锁持有时间**: 全局锁仅在数据修改和关键同步点持有
5. **自适应优化**: 根据负载动态调整自旋策略和异步命令启用条件

这种设计使得 KeyDB 在多核机器上能够获得显著的吞吐量提升，同时保持了与 Redis 的完全协议兼容性。
