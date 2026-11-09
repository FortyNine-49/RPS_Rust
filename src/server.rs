//! 服务器接入层：监听端口、accept 连接、虚拟主机/location 路由和每连接主循环。
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};

use crate::config::{Config, LocationConfig, ServerConfig, UseMode};
use crate::http::{self, Request, Version};
use crate::log;
use crate::proxy;
use crate::upstream::{UpstreamGroup, UpstreamPool};

// 客户端连接空闲超时。
const CLIENT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// 全局共享运行上下文：配置、upstream 组、连接池和连接数限制。
pub struct App {
    pub cfg: Arc<Config>,
    pub upstreams: HashMap<String, Arc<UpstreamGroup>>,
    pub pool: UpstreamPool,
    pub conn_limit: Arc<Semaphore>,
}

/// App 的构造。
impl App {
    /// 创建 upstream 运行时组和连接信号量。
    pub fn new(cfg: Arc<Config>) -> Self {
        let upstreams = cfg
            .http
            .upstreams
            .iter()
            .map(|u| (u.name.clone(), Arc::new(UpstreamGroup::new(u.clone()))))
            .collect();
        let permits = cfg
            .worker_processes
            .saturating_mul(cfg.worker_connections)
            .max(1);
        Self {
            cfg,
            upstreams,
            pool: UpstreamPool::new(),
            conn_limit: Arc::new(Semaphore::new(permits)),
        }
    }
}

/// 一个监听地址及其对应的 server 配置索引。
struct ListenerSpec {
    ip: IpAddr,
    port: u16,
    servers: Vec<usize>,
}

/// 启动监听、accept 循环、空闲连接清理，并等待关闭信号。
pub async fn run(cfg: Arc<Config>, shutdown: watch::Receiver<bool>) -> std::io::Result<()> {
    let specs = build_listener_specs(&cfg);
    if specs.is_empty() {
        log::emerg("no server has a listen directive; nothing to serve");
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no listeners",
        ));
    }

    let app = Arc::new(App::new(cfg.clone()));
    let mut accept_tasks = Vec::new();
    let mut listener_count = 0usize;

    for spec in specs {
        let listener = bind_listener(spec.ip, spec.port).await.map_err(|e| {
            log::emerg(format!("failed to bind {}:{}: {}", spec.ip, spec.port, e));
            e
        })?;
        log::info(format!(
            "listening on {}:{} ({} server block(s))",
            spec.ip,
            spec.port,
            spec.servers.len()
        ));
        listener_count += 1;

        let listener = Arc::new(listener);
        let servers = Arc::new(spec.servers);
        let port = spec.port;

        for _ in 0..cfg.worker_processes.max(1) {
            let listener = listener.clone();
            let app = app.clone();
            let servers = servers.clone();
            let shutdown = shutdown.clone();
            let mode = cfg.use_mode;
            accept_tasks.push(tokio::spawn(async move {
                accept_loop(listener, app, servers, port, shutdown, mode).await;
            }));
        }
    }

    if listener_count == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no listening sockets",
        ));
    }

    {
        let app = app.clone();
        let mut shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.changed() => break,
                    _ = tokio::time::sleep(Duration::from_secs(15)) => {
                        let timeouts: HashMap<String, Duration> = app
                            .upstreams
                            .iter()
                            .map(|(name, g)| (name.clone(), g.cfg.keepalive_timeout))
                            .collect();
                        app.pool.prune(&timeouts);
                        let idle = app.pool.idle_count();
                        if idle > 0 {
                            log::debug(format!("upstream keepalive pool: {} idle connection(s)", idle));
                        }
                    }
                }
            }
        });
    }

    wait_for_shutdown(shutdown).await;

    for task in accept_tasks {
        task.abort();
    }
    log::info("server stopped");
    Ok(())
}

/// 等待 shutdown watch 变为 true。
async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

/// 按绑定 IP 和端口合并 server 块。
fn build_listener_specs(cfg: &Config) -> Vec<ListenerSpec> {
    let mut map: HashMap<(IpAddr, u16), Vec<usize>> = HashMap::new();
    for (idx, server) in cfg.http.servers.iter().enumerate() {
        let ip = server
            .listen_ip
            .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        map.entry((ip, server.listen_port)).or_default().push(idx);
    }
    let mut specs: Vec<ListenerSpec> = map
        .into_iter()
        .map(|((ip, port), servers)| ListenerSpec { ip, port, servers })
        .collect();
    specs.sort_by_key(|a| (a.port, a.ip.to_string()));
    specs
}

/// 创建并绑定 TCP 监听 socket。
async fn bind_listener(ip: IpAddr, port: u16) -> std::io::Result<TcpListener> {
    let socket = match ip {
        IpAddr::V4(_) => TcpSocket::new_v4()?,
        IpAddr::V6(_) => TcpSocket::new_v6()?,
    };
    socket.set_reuseaddr(true)?;
    socket.bind(SocketAddr::new(ip, port))?;
    socket.listen(511)
}

/// 接收连接，按执行模型交给异步任务或独立线程。
async fn accept_loop(
    listener: Arc<TcpListener>,
    app: Arc<App>,
    servers: Arc<Vec<usize>>,
    local_port: u16,
    mut shutdown: watch::Receiver<bool>,
    mode: UseMode,
) {
    loop {
        if *shutdown.borrow() {
            break;
        }

        let accepted = tokio::select! {
            _ = shutdown.changed() => break,
            res = listener.accept() => res,
        };

        let (stream, peer) = match accepted {
            Ok(v) => v,
            Err(e) => {
                log::warn(format!("accept failed: {}", e));
                continue;
            }
        };

        let permit = match app.conn_limit.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                log::warn("connection limit reached, dropping new connection");
                drop(stream);
                continue;
            }
        };

        let app2 = app.clone();
        let servers2 = servers.clone();
        let peer_ip = peer.ip().to_string();

        match mode {
            UseMode::Reactor => {
                tokio::spawn(async move {
                    log::debug(format!("accepted connection from {}", peer_ip));
                    if let Err(e) =
                        handle_connection(stream, peer, local_port, servers2, app2, permit).await
                    {
                        log::debug(format!("connection {} ended: {}", peer_ip, e));
                    }
                });
            }
            UseMode::Thread => {
                let std_stream = match stream.into_std() {
                    Ok(s) => s,
                    Err(e) => {
                        log::warn(format!("failed to detach client stream: {}", e));
                        continue;
                    }
                };
                std::thread::spawn(move || {
                    let rt = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(e) => {
                            log::error(format!("failed to create connection runtime: {}", e));
                            return;
                        }
                    };
                    rt.block_on(async move {
                        let stream = match TcpStream::from_std(std_stream) {
                            Ok(s) => s,
                            Err(e) => {
                                log::warn(format!("failed to attach client stream: {}", e));
                                return;
                            }
                        };
                        log::debug(format!("accepted connection from {}", peer_ip));
                        if let Err(e) =
                            handle_connection(stream, peer, local_port, servers2, app2, permit)
                                .await
                        {
                            log::debug(format!("connection {} ended: {}", peer_ip, e));
                        }
                    });
                });
            }
        }
    }
}

/// 单个客户端的 keepalive 主循环。
async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    local_port: u16,
    servers: Arc<Vec<usize>>,
    app: Arc<App>,
    _permit: OwnedSemaphorePermit,
) -> std::io::Result<()> {
    let _ = stream.set_nodelay(true);
    let mut reader = BufReader::new(stream);

    loop {
        let head =
            match tokio::time::timeout(CLIENT_IDLE_TIMEOUT, http::read_request_head(&mut reader))
                .await
            {
                Ok(Ok(Some(head))) => head,
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    log::info(format!("bad request from {}: {}", peer.ip(), e.message));
                    send_simple_error(&mut reader, e.status, &e.message).await;
                    break;
                }
                Err(_) => {
                    log::debug(format!("client {} idle timeout", peer.ip()));
                    break;
                }
            };

        let server = match select_server(&app.cfg, &servers, head.header("host")) {
            Some(s) => s,
            None => {
                send_simple_error(&mut reader, 500, "no virtual host configured").await;
                break;
            }
        };
        let location = select_location(server, &head.path);
        let body_limit = location
            .and_then(|l| l.body_max_size)
            .or(server.body_max_size)
            .or(app.cfg.http.body_max_size)
            .unwrap_or(http::DEFAULT_BODY_MAX);

        if head.expect_continue {
            if reader
                .get_mut()
                .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
                .await
                .is_err()
            {
                break;
            }
            let _ = reader.get_mut().flush().await;
        }

        let body = match tokio::time::timeout(
            CLIENT_IDLE_TIMEOUT,
            http::read_request_body(&mut reader, &head, body_limit),
        )
        .await
        {
            Ok(Ok(body)) => body,
            Ok(Err(e)) => {
                log::info(format!(
                    "request body error from {}: {}",
                    peer.ip(),
                    e.message
                ));
                send_simple_error(&mut reader, e.status, &e.message).await;
                break;
            }
            Err(_) => {
                send_simple_error(&mut reader, 408, "request body read timed out").await;
                break;
            }
        };

        let req = Request { head, body };

        if let Some(loc) = location {
            if let Some(proxy_cfg) = &loc.proxy {
                if proxy::is_websocket_request(&req) {
                    let _ = proxy::handle_websocket(
                        reader,
                        &app,
                        &req,
                        proxy_cfg,
                        peer.ip(),
                        local_port,
                        server,
                    )
                    .await;
                    return Ok(());
                }

                let outcome = proxy::handle_http(
                    reader.get_mut(),
                    &app,
                    &req,
                    proxy_cfg,
                    peer.ip(),
                    local_port,
                    server,
                )
                .await;

                if !outcome.client_keepalive {
                    break;
                }
                continue;
            }
        }

        let keepalive = req.head.keepalive;
        let body = b"Hello from RPS!\n";
        match http::write_simple_response(
            reader.get_mut(),
            req.head.version,
            200,
            "OK",
            body,
            keepalive,
            &[],
        )
        .await
        {
            Ok(()) => {}
            Err(_) => break,
        }
        if !keepalive {
            break;
        }
    }

    Ok(())
}

/// 向客户端发送简单错误响应并关闭连接。
async fn send_simple_error(reader: &mut BufReader<TcpStream>, status: u16, message: &str) {
    let reason = http::reason_phrase(status);
    let body = format!(
        "<html><head><title>{} {}</title></head><body><h1>{} {}</h1><p>{}</p></body></html>\n",
        status,
        reason,
        status,
        reason,
        message.replace('&', "&amp;").replace('<', "&lt;")
    );
    let _ = http::write_simple_response(
        reader.get_mut(),
        Version::Http11,
        status,
        reason,
        body.as_bytes(),
        false,
        &[],
    )
    .await;
}

/// 根据监听端口和 Host 选择虚拟主机。
fn select_server<'a>(
    cfg: &'a Config,
    servers: &[usize],
    host: Option<&str>,
) -> Option<&'a ServerConfig> {
    let host = host.unwrap_or("");
    if host.is_empty() {
        return servers.first().map(|&i| &cfg.http.servers[i]);
    }

    for &idx in servers {
        let s = &cfg.http.servers[idx];
        if let Some(name) = &s.server_name {
            if server_name_matches(name, host) {
                return Some(s);
            }
        }
    }

    servers.first().map(|&i| &cfg.http.servers[i])
}

/// 比较 Host 与 server_name，忽略大小写和端口。
fn server_name_matches(name: &str, host: &str) -> bool {
    name.eq_ignore_ascii_case(host) || strip_port(name).eq_ignore_ascii_case(&strip_port(host))
}

/// 去掉 host 中的端口部分。
fn strip_port(host: &str) -> String {
    if host.starts_with('[') {
        if let Some(end) = host.find(']') {
            return host[..=end].to_string();
        }
    }
    if let Some((h, p)) = host.rsplit_once(':') {
        if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) {
            return h.to_string();
        }
    }
    host.to_string()
}

/// 按最长前缀匹配 location，未命中时回退第一个。
fn select_location<'a>(server: &'a ServerConfig, path: &str) -> Option<&'a LocationConfig> {
    let mut best: Option<&LocationConfig> = None;
    for loc in &server.locations {
        if path.starts_with(&loc.pattern) {
            let better = best
                .map(|b| loc.pattern.len() > b.pattern.len())
                .unwrap_or(true);
            if better {
                best = Some(loc);
            }
        }
    }

    best.or_else(|| server.locations.first())
}
