# RPS — Rust

这是 `https://github.com/Jackdmw/RPS`的 Rust 重写实现。保留了原项目的配置语法、HTTP/1.1 反向代理、WebSocket 透传、平滑加权轮询、upstream keepalive 连接池、虚拟主机 / location 路由和 `-c/-l/-p/-t/-s stop` 命令行接口，同时在 Rust 侧补齐了若干协议细节并做到跨平台运行。

> 原项目：`https://github.com/Jackdmw/RPS`

## 快速开始

要求：Rust stable（已在 1.99.1 上验证），无需 C 编译器或系统库。

```sh
cargo build --release

# 前台运行
./target/release/rps -c ./configs/reactor.conf -l 8

# 测试配置
./target/release/rps -c ./configs/reactor.conf -t

# 停止 daemon（配置里 daemon on 时）
./target/release/rps -c ./configs/reactor.conf -s stop
```

## 功能

- **HTTP/1.1 反向代理**：请求行 / 头部解析、请求体与响应体转发、keepalive、hop-by-hop 头过滤。
- **WebSocket 代理**：自动识别 `Upgrade: websocket`，透传 101 握手，随后原始字节全双工转发。
- **负载均衡**：平滑加权轮询（WRR），与原始算法一致；支持 `backup`、`down`、`max_fails`、`fail_timeout`，连接失败时自动尝试下一个 peer。
- **upstream keepalive 连接池**：LIFO 复用、按 `host:port` 隔离（修复了原实现跨节点复用连接的隐患）、空闲超时清理、`keepalive_requests` 上限。
- **双执行模型**：
  - `use epoll`（或 `use io_uring`）→ Tokio 多线程 Reactor；Linux 上使用 epoll，Windows 上使用 IOCP。
  - `use multithreading` → 每个客户端连接一个独立 OS 线程（线程内使用 current-thread 运行时），贴近原项目的一连接一线程模型。
- **CLI / daemon**：`-c`、`-l 0..8`、`-p`、`-t`、`-s stop`；PID 文件锁、分离式 daemon、优雅停止。
- **协议健壮性**：支持 chunked 请求/响应、`Expect: 100-continue`、HEAD/204/304 无 body 判定、请求 smuggling 基础校验、body 大小限制。
- **`proxy_set_header` 变量展开**：支持 `$host`、`$proxy_host`、`$proxy_port`、`$remote_addr`、`$proxy_add_x_forwarded_for`、`$request_uri`、`$scheme`、`$server_name`、`$server_port`、`$http_<header>`

## 命令行

| 参数 | 说明 |
|------|------|
| `-c <file>` | 配置文件路径（必需） |
| `-l <0-8>` | 日志级别，0 静默、1 emerg … 8 debug（默认 8） |
| `-p <path>` | 运行前缀，用于解析相对 pid 路径；默认使用配置文件所在目录 |
| `-t` | 只校验配置并退出 |
| `-s stop` | 读取 pid 文件并停止 daemon |

`-t`、`-s stop` 都需要先成功解析配置文件。

## 配置示例

```nginx
worker_processes 4;
daemon off;
pid run_pid.conf;

event {
    use epoll;                 # epoll / multithreading
    worker_connections 1024;
}

http {
    upstream backend {
        server 127.0.0.1:8080 weight=3 max_fails=2 fail_timeout=10s;
        server 127.0.0.1:8081 backup;
        keepalive 32;
        keepalive_timeout 60s;   # 支持 ms/s，裸数字为毫秒
        keepalive_requests 100;
    }

    server {
        listen 8000;              # 也可写 listen 127.0.0.1:8000
        server_name "example.com";

        location / {
            proxy_pass http://backend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
        }

        location /api {
            proxy_pass http://127.0.0.1:9090/api;
            proxy_connect_timeout 30000;
            proxy_read_timeout 30000;
            proxy_send_timeout 30000;
        }

        location /ws {
            proxy_pass http://backend;
        }

        location /static {
            proxy_set_header X-Ignored yes;   # 没有 proxy_pass → 返回内置 Hello
        }
    }
}
```

### 指令表

| 指令 | 上下文 | 默认值 | 说明 |
|------|--------|--------|------|
| `worker_processes <n\|auto>` | main | 1 | Reactor 模式的运行时工作线程数 / 各模式 accept loop 数 |
| `daemon on\|off` | main | off | 分离式后台进程 |
| `pid <file>` | main | `run_pid.conf` | PID 文件；相对路径基于 prefix |
| `event { }` | main | — | 事件配置块 |
| `use epoll\|io_uring\|multithreading` | event | epoll | `io_uring` 与原项目一样未真正实现，按 Reactor 处理 |
| `worker_connections <n>` | event | 512 | 每 worker 连接上限；总上限 = worker_processes × n |
| `http { }` | main | — | HTTP 配置块 |
| `http_body_max_size <size>` | http / server / location | 64 MiB（内部安全上限） | 请求体上限；支持 k/m/g 后缀 |
| `upstream <name> { }` | http | — | 后端组 |
| `server <addr> [参数]` | upstream | — | `weight=`、`max_fails=`、`fail_timeout=`、`down`、`backup` |
| `keepalive <n>` | upstream | 0（禁用） | 每个 upstream 组的空闲连接缓存上限 |
| `keepalive_timeout <time>` | upstream | 60000ms | 空闲连接超时 |
| `keepalive_requests <n>` | upstream | 100 | 单连接最大请求数 |
| `server { }` | http | — | 虚拟主机 |
| `listen <port\|addr:port>` | server | — | 监听地址（原始实现固定 0.0.0.0，这里支持指定 IP） |
| `server_name <name>` | server | — | Host 匹配，大小写不敏感；比较时会忽略端口 |
| `location <prefix> { }` | server | — | 最长前缀匹配；无匹配时回退到第一个 location |
| `proxy_pass <url>` | location | — | `http://host[:port][/path]`，或省略协议写 `backend` |
| `proxy_set_header <key> <value>` | location | — | 可重复；支持变量展开 |
| `proxy_connect_timeout <time>` | location | 60000ms | 连接后端超时 |
| `proxy_read_timeout <time>` | location | 60000ms | 读取后端响应超时 |
| `proxy_send_timeout <time>` | location | 60000ms | 发送请求 / 写客户端超时 |
| `proxy_buffering on\|off` | location | on | 保留解析；实现始终流式转发 |
| `proxy_pass_request_headers on\|off` | location | on | 是否透传客户端原始头 |
| `proxy_pass_request_body on\|off` | location | on | 是否转发请求体 |

### `proxy_pass` 的路径语义

- `proxy_pass http://backend;`：原样转发客户端 URI（含 query）。
- `proxy_pass http://backend/headers;`：用 `/headers` 替换客户端路径，客户端 query 会追加为 `?query`（若已有 query 则用 `&` 连接）。
- `proxy_pass http://backend;` 中的 `backend` 若存在同名 `upstream` 块则走负载均衡；否则把它当作主机名直连（端口默认 80）。

### 注意

- 不支持 HTTPS/TLS；配置中出现 `https://` 会在解析阶段报错。
- 请求体会读入内存后转发，默认上限 64 MiB；响应体是流式转发，不受此限制。
- `proxy_buffering` 仅为配置兼容保留，实际始终采用流式响应转发。

## 项目结构

```text
RPS_Rust/
├── Cargo.toml                     # 包信息与依赖：tokio、fs2；Unix 下额外依赖 libc
├── Cargo.lock                     # 锁定依赖的精确版本（二进制项目应提交）
├── README.md                      # 项目说明、配置参考、架构与测试文档
│
├── configs/                       # 可直接使用的配置样例
│   ├── reactor.conf               # Reactor 模式：upstream、keepalive、/api、/ws
│   ├── thread.conf                # 一连接一线程（multithreading）模式
│   └── minimal.conf               # 最小反向代理配置
│
├── src/                           # Rust 源码
│   ├── main.rs                    # 程序入口：CLI、daemon、PID 文件锁、信号、runtime 选择
│   ├── config.rs                  # nginx 风格词法 / 语法 / 语义解析与校验，生成 Config
│   ├── log.rs                     # 0..8 分级日志（stderr）
│   ├── http.rs                    # HTTP/1.x 读写、头部校验、body framing、错误/默认响应
│   ├── upstream.rs                # 平滑 WRR、peer 健康状态、keepalive 连接池
│   ├── proxy.rs                   # 上游请求构造、响应转发、chunked 中继、WebSocket 隧道
│   └── server.rs                  # 监听 socket、虚拟主机 / location 路由、每连接循环
│
└── test/                          # 端到端测试
    ├── backend.py                 # HTTP 测试后端：keepalive、/headers、/echo、/large、/chunked…
    ├── ws_backend.py              # WebSocket 测试后端：握手、echo、ping/pong
    └── integration_test.py        # 测试总控：启动后端 / 生成配置 / 运行 RPS / 22 项断言
```

运行时还会在配置目录生成：

| 文件 | 说明 | 生命周期 |
|------|------|----------|
| `configs/run_pid.conf` | 当前进程 PID | 启动时写入，正常退出时删除 |
| `configs/run_pid.conf.lock` | 实例互斥锁 | 随 PID 文件一起创建/删除 |
| `test/rps_it_*` | 集成测试临时目录与日志 | 测试成功自动删除，失败保留便于排查 |

### 两种执行模型

| | `use epoll` | `use multithreading` |
|---|---|---|
| I/O | Tokio 异步非阻塞（Linux epoll / Windows IOCP） | 每连接一个 OS 线程，线程内 current-thread 异步 I/O |
| 并发 | 多 worker 运行时 + 多 accept loop | accept loop + 线程/连接 |
| 连接池 | 共享 `Mutex` 连接池 | 同一共享连接池 |
| 适用 | 高并发、大量空闲连接 | 贴近原项目“一连接一线程”语义、便于同步调试 |

原项目通过 `fork()` 创建 master/worker 进程；Rust 版本为避免 POSIX 专属 fork，使用线程化 worker。daemon 模式改为启动一个带 `RPS_DAEMONIZED=1` 的分离子进程；PID 文件通过独立 `.lock` 文件加锁。

## 测试

单元测试（配置解析、HTTP 解析、WRR、backup、失败冷却）：

```sh
cargo test
```

端到端测试（两个 HTTP 后端 + 一个 WebSocket 后端，覆盖 Reactor / 线程模式 / daemon）：

```sh
# 需要 Python 3；uv 环境也可
uv run python ./test/integration_test.py
# 或：python ./test/integration_test.py
```

测试内容：配置校验、基本代理、响应头、2:1 WRR 分布、upstream keepalive 复用、POST body、路径替换 + query、chunked 响应、404 透传、大响应、`proxy_set_header` / XFF、`proxy_pass_request_headers off`、默认响应、502、WebSocket echo/ping、线程模式、daemon 启停。

## 与原 C 版本的差异 / 改进

- 跨平台构建；不需要 Linux/epoll/pthread。
- 无需 `fork`，daemon 用分离子进程实现。
- 修复原 upstream keepalive 缓存未按后端地址区分、可能把连接复用给错误 peer 的问题。
- 连接失败会按 `max_fails` / `fail_timeout` 记录并尝试下一个 peer；过期缓存连接会自动重建。
- 支持 chunked 请求与响应、`Expect: 100-continue`、HEAD/204/304、大小限制和请求 smuggling 校验。
- `proxy_set_header` 支持变量展开。
- 响应始终流式转发；请求体因上游重试需要而缓冲。

## License

Apache License 2.0，见 [LICENSE](LICENSE)。本项目基于 `https://github.com/Jackdmw/RPS` 重写。
