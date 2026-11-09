//! 代理核心：构造上游请求、选择节点、转发响应、chunked 中继和 WebSocket 隧道。
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::config::{ProxyConfig, ServerConfig};
use crate::http::{self, BodyFraming, HttpError, Request, ResponseHead};
use crate::log;
use crate::server::App;
use crate::upstream::UpstreamGroup;

// 转发响应体时使用的缓冲区大小。
const RELAY_BUF: usize = 16 * 1024;

// 转发请求时需要跳过的连接级和协议级头。
const SKIP_REQUEST_HEADERS: &[&str] = &[
    "host",
    "connection",
    "content-length",
    "transfer-encoding",
    "expect",
    "keep-alive",
    "proxy-connection",
    "te",
    "trailer",
    "upgrade",
];

// WebSocket 握手头单独构造，转发原始头时跳过。
const SKIP_WS_HEADERS: &[&str] = &[
    "sec-websocket-key",
    "sec-websocket-version",
    "sec-websocket-protocol",
    "sec-websocket-extensions",
];

/// 代理结果：客户端连接是否可继续 keepalive。
pub struct ProxyOutcome {
    pub client_keepalive: bool,
}

/// 判断请求是否为 WebSocket 升级请求。
pub fn is_websocket_request(req: &Request) -> bool {
    req.header_contains_token("upgrade", "websocket")
        && req.header_contains_token("connection", "upgrade")
}

/// 处理一次 HTTP 代理请求，负责选节点、重试和错误响应。
pub async fn handle_http<W: AsyncWrite + Unpin>(
    writer: &mut W,
    app: &App,
    req: &Request,
    proxy: &ProxyConfig,
    remote_ip: IpAddr,
    local_port: u16,
    server: &ServerConfig,
) -> ProxyOutcome {
    let group = resolve_group(app, proxy);
    let mut excluded: Vec<usize> = Vec::new();
    let mut direct_used = false;
    let mut last_error = HttpError::upstream("no available upstream peer");

    'candidates: loop {
        let cand = if let Some(g) = group {
            match g.select(&excluded) {
                Some(idx) => {
                    excluded.push(idx);
                    Some(idx)
                }
                None => break,
            }
        } else {
            if direct_used {
                break;
            }
            direct_used = true;
            None
        };

        let (host, port) = match cand {
            Some(idx) => {
                let g = group.expect("peer index without group");
                let (h, p) = g.address(idx);
                (h.to_string(), p)
            }
            None => (proxy.target_host.clone(), proxy.target_port),
        };
        let is_group = group.is_some();
        let keepalive_to_upstream = group.map(|g| g.cfg.keepalive > 0).unwrap_or(false);
        let request_bytes = build_http_request(
            req,
            proxy,
            &host,
            port,
            is_group,
            keepalive_to_upstream,
            remote_ip,
            local_port,
            server,
        );

        let mut retried_fresh = false;
        loop {
            match attempt_http(
                writer,
                app,
                req,
                proxy,
                group,
                cand,
                &host,
                port,
                request_bytes.clone(),
                !retried_fresh,
            )
            .await
            {
                Ok(outcome) => {
                    if let (Some(g), Some(idx)) = (group, cand) {
                        g.mark_success(idx);
                    }
                    return outcome;
                }
                Err(failure) => {
                    if failure.used_pooled
                        && !failure.response_started
                        && failure.error.status == 502
                        && !retried_fresh
                    {
                        log::debug(format!(
                            "proxy: cached upstream connection was stale ({}); retrying",
                            failure.error.message
                        ));
                        retried_fresh = true;
                        continue;
                    }

                    if let (Some(g), Some(idx)) = (group, cand) {
                        g.mark_failure(idx);
                    }
                    log::warn(format!(
                        "proxy: upstream {}:{} failed: {}",
                        host, port, failure.error.message
                    ));
                    last_error = failure.error;

                    if failure.connect_failed {
                        continue 'candidates;
                    }
                    if !failure.response_started {
                        send_error(writer, req, &last_error).await;
                    }
                    return ProxyOutcome {
                        client_keepalive: false,
                    };
                }
            }
        }
    }

    send_error(writer, req, &last_error).await;
    ProxyOutcome {
        client_keepalive: false,
    }
}

/// 根据 proxy_pass 名称查找 upstream 组。
fn resolve_group<'a>(app: &'a App, proxy: &ProxyConfig) -> Option<&'a Arc<UpstreamGroup>> {
    proxy
        .upstream_name
        .as_ref()
        .and_then(|name| app.upstreams.get(name))
}

/// 单次上游尝试的失败信息。
struct AttemptFailure {
    error: HttpError,
    connect_failed: bool,
    used_pooled: bool,
    response_started: bool,
}

/// 已建立的上游连接及池相关状态。
struct OpenConn {
    reader: BufReader<TcpStream>,
    used_pooled: bool,
    uses: usize,
}

/// 优先从连接池取连接，否则新建 TCP 连接。
async fn open_connection(
    app: &App,
    group: Option<&Arc<UpstreamGroup>>,
    host: &str,
    port: u16,
    connect_timeout: Duration,
    allow_pool: bool,
) -> Result<OpenConn, HttpError> {
    if allow_pool {
        if let Some(g) = group {
            if g.cfg.keepalive > 0 {
                for _ in 0..8 {
                    match app
                        .pool
                        .take(&g.cfg.name, host, port, g.cfg.keepalive_timeout)
                    {
                        Some((stream, uses)) => {
                            if uses >= g.cfg.keepalive_requests {
                                drop(stream);
                                continue;
                            }
                            if stream_looks_alive(&stream) {
                                let _ = stream.set_nodelay(true);
                                return Ok(OpenConn {
                                    reader: BufReader::new(stream),
                                    used_pooled: true,
                                    uses,
                                });
                            }
                            drop(stream);
                        }
                        None => break,
                    }
                }
            }
        }
    }

    let connect = TcpStream::connect((host, port));
    let stream = tokio::time::timeout(connect_timeout, connect)
        .await
        .map_err(|_| {
            HttpError::new(
                502,
                format!(
                    "connect to {}:{} timed out after {:?}",
                    host, port, connect_timeout
                ),
            )
        })?
        .map_err(|e| HttpError::new(502, format!("connect to {}:{} failed: {}", host, port, e)))?;
    let _ = stream.set_nodelay(true);
    Ok(OpenConn {
        reader: BufReader::new(stream),
        used_pooled: false,
        uses: 0,
    })
}

/// 用非阻塞读探测缓存连接是否仍然可用。
fn stream_looks_alive(stream: &TcpStream) -> bool {
    let mut byte = [0u8; 1];
    matches!(
        stream.try_read(&mut byte),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
    )
}

/// 完成一次完整上游尝试：发送请求、读取并转发响应。
#[allow(clippy::too_many_arguments)]
async fn attempt_http<W: AsyncWrite + Unpin>(
    writer: &mut W,
    app: &App,
    req: &Request,
    proxy: &ProxyConfig,
    group: Option<&Arc<UpstreamGroup>>,
    peer_idx: Option<usize>,
    host: &str,
    port: u16,
    request_bytes: Vec<u8>,
    allow_pool: bool,
) -> Result<ProxyOutcome, AttemptFailure> {
    let mut conn =
        match open_connection(app, group, host, port, proxy.connect_timeout, allow_pool).await {
            Ok(c) => c,
            Err(e) => {
                return Err(AttemptFailure {
                    error: e,
                    connect_failed: true,
                    used_pooled: false,
                    response_started: false,
                })
            }
        };

    conn.uses += 1;

    if let Err(e) = write_upstream(conn.reader.get_mut(), &request_bytes, proxy.send_timeout).await
    {
        return Err(AttemptFailure {
            error: e,
            connect_failed: false,
            used_pooled: conn.used_pooled,
            response_started: false,
        });
    }

    let head = match tokio::time::timeout(
        proxy.read_timeout,
        http::read_response_head(&mut conn.reader),
    )
    .await
    {
        Ok(Ok(h)) => h,
        Ok(Err(e)) => {
            return Err(AttemptFailure {
                error: e,
                connect_failed: false,
                used_pooled: conn.used_pooled,
                response_started: false,
            })
        }
        Err(_) => {
            return Err(AttemptFailure {
                error: HttpError::new(
                    502,
                    format!(
                        "upstream {}:{} response timed out after {:?}",
                        host, port, proxy.read_timeout
                    ),
                ),
                connect_failed: false,
                used_pooled: conn.used_pooled,
                response_started: false,
            })
        }
    };

    let framing = match http::response_framing(&req.head.method, &head) {
        Ok(f) => f,
        Err(e) => {
            return Err(AttemptFailure {
                error: e,
                connect_failed: false,
                used_pooled: false,
                response_started: false,
            })
        }
    };

    let client_keepalive =
        req.head.keepalive && framing != BodyFraming::UntilEof && head.status != 101;

    if let Err(e) = forward_response(
        writer,
        req,
        &head,
        &mut conn.reader,
        framing,
        proxy,
        client_keepalive,
    )
    .await
    {
        return Err(AttemptFailure {
            error: e,
            connect_failed: false,
            used_pooled: false,
            response_started: true,
        });
    }

    if let (Some(g), Some(_idx)) = (group, peer_idx) {
        let upstream_keepalive = http::response_keepalive(&head, framing);
        if upstream_keepalive
            && g.cfg.keepalive > 0
            && conn.uses <= g.cfg.keepalive_requests
            && conn.reader.buffer().is_empty()
        {
            let uses = conn.uses;
            let timeout = g.cfg.keepalive_timeout;
            let stream = conn.reader.into_inner();
            app.pool.put(
                &g.cfg.name,
                host,
                port,
                stream,
                uses,
                g.cfg.keepalive,
                timeout,
            );
        }
    }

    Ok(ProxyOutcome { client_keepalive })
}

/// 把后端响应头和 body 流式转发给客户端。
async fn forward_response<W: AsyncWrite + Unpin, R: AsyncBufRead + Unpin>(
    writer: &mut W,
    req: &Request,
    head: &ResponseHead,
    reader: &mut R,
    framing: BodyFraming,
    proxy: &ProxyConfig,
    client_keepalive: bool,
) -> Result<(), HttpError> {
    let mut out = Vec::with_capacity(512);
    out.extend_from_slice(req.head.version.as_str().as_bytes());
    out.extend_from_slice(format!(" {} {}\r\n", head.status, head.reason).as_bytes());

    let conn_tokens = http::connection_tokens(&head.headers);
    let mut server_seen = false;

    for h in &head.headers {
        let lname = h.name.to_ascii_lowercase();
        if lname == "transfer-encoding" {
            continue;
        }
        if lname == "content-length" {
            if matches!(framing, BodyFraming::Length(_) | BodyFraming::None) {
                out.extend_from_slice(h.name.as_bytes());
                out.extend_from_slice(b": ");
                out.extend_from_slice(http::sanitize_header_value(&h.value).as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            continue;
        }
        if lname == "server" {
            server_seen = true;
        }
        if http::is_hop_by_hop(&lname) || conn_tokens.iter().any(|t| t == &lname) {
            continue;
        }
        out.extend_from_slice(h.name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(http::sanitize_header_value(&h.value).as_bytes());
        out.extend_from_slice(b"\r\n");
    }

    if !server_seen {
        out.extend_from_slice(b"Server: RPS\r\n");
    }
    if framing == BodyFraming::Chunked {
        out.extend_from_slice(b"Transfer-Encoding: chunked\r\n");
    }
    if client_keepalive {
        out.extend_from_slice(b"Connection: keep-alive\r\nKeep-Alive: timeout=60\r\n");
    } else {
        out.extend_from_slice(b"Connection: close\r\n");
    }
    out.extend_from_slice(b"\r\n");

    write_client(writer, &out, proxy.send_timeout).await?;

    match framing {
        BodyFraming::None => {}
        BodyFraming::Length(n) => {
            relay_exact(reader, writer, n, proxy.read_timeout, proxy.send_timeout).await?
        }
        BodyFraming::Chunked => {
            relay_chunked(reader, writer, proxy.read_timeout, proxy.send_timeout).await?
        }
        BodyFraming::UntilEof => {
            relay_until_eof(reader, writer, proxy.read_timeout, proxy.send_timeout).await?
        }
    }

    flush_client(writer, proxy.send_timeout).await
}

/// 向客户端发送代理错误响应。
pub async fn send_error<W: AsyncWrite + Unpin>(writer: &mut W, req: &Request, err: &HttpError) {
    if err.status == 499 {
        return;
    }
    let status = if err.status == 0 { 502 } else { err.status };
    let reason = http::reason_phrase(status);
    let body = format!(
        "<html><head><title>{} {}</title></head><body><h1>{} {}</h1><p>{}</p></body></html>\n",
        status,
        reason,
        status,
        reason,
        escape_html(&err.message)
    );
    let _ = http::write_simple_response(
        writer,
        req.head.version,
        status,
        reason,
        body.as_bytes(),
        false,
        &[],
    )
    .await;
}

/// 把 HttpError 转换为 io::Error。
fn http_error_to_io(e: HttpError) -> std::io::Error {
    std::io::Error::other(e.message)
}

/// 转义错误页中的 HTML 特殊字符。
fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

/// 构造发往后端的 HTTP 请求。
#[allow(clippy::too_many_arguments)]
fn build_http_request(
    req: &Request,
    proxy: &ProxyConfig,
    upstream_host: &str,
    upstream_port: u16,
    is_group: bool,
    keepalive_to_upstream: bool,
    remote_ip: IpAddr,
    local_port: u16,
    server: &ServerConfig,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(1024 + req.body.len());
    let path = build_target_path(proxy, req);
    let host_header = build_host_header(upstream_host, upstream_port, is_group);
    let ctx = VarCtx {
        req,
        remote_ip,
        upstream_port,
        host_header: &host_header,
        server,
        local_port,
    };
    let set_names: HashSet<String> = proxy
        .set_headers
        .iter()
        .map(|(k, _)| k.to_ascii_lowercase())
        .collect();

    out.extend_from_slice(req.head.method.as_bytes());
    out.push(b' ');
    out.extend_from_slice(path.as_bytes());
    out.extend_from_slice(b" HTTP/1.1\r\n");

    push_header(&mut out, "Host", &host_header);

    if req.header("x-real-ip").is_none() && !set_names.contains("x-real-ip") {
        push_header(&mut out, "X-Real-IP", &remote_ip.to_string());
    }

    let existing_xff = req.header_all("x-forwarded-for").join(", ");
    if !set_names.contains("x-forwarded-for") {
        let xff = if existing_xff.is_empty() {
            remote_ip.to_string()
        } else {
            format!("{}, {}", existing_xff, remote_ip)
        };
        push_header(&mut out, "X-Forwarded-For", &xff);
    }

    for (key, value) in &proxy.set_headers {
        push_header(&mut out, key, &expand_vars(value, &ctx));
    }

    append_origin_headers(&mut out, req, proxy, &set_names, false);

    if proxy.pass_request_body
        && (req.head.content_length.is_some() || req.head.chunked || !req.body.is_empty())
    {
        push_header(&mut out, "Content-Length", &req.body.len().to_string());
    }

    if keepalive_to_upstream {
        out.extend_from_slice(b"Connection: keep-alive\r\n");
    } else {
        out.extend_from_slice(b"Connection: close\r\n");
    }
    out.extend_from_slice(b"\r\n");

    if proxy.pass_request_body {
        out.extend_from_slice(&req.body);
    }
    out
}

/// 计算转发给后端的路径，并拼接 query。
fn build_target_path(proxy: &ProxyConfig, req: &Request) -> String {
    let (base, has_query) = match &proxy.target_uri {
        Some(t) => (t.clone(), t.contains('?')),
        None => (req.head.path.clone(), false),
    };
    if req.head.query.is_empty() {
        base
    } else if has_query {
        format!("{}&{}", base, req.head.query)
    } else {
        format!("{}?{}", base, req.head.query)
    }
}

/// 根据直连或 upstream 组构造 Host 头。
fn build_host_header(host: &str, port: u16, is_group: bool) -> String {
    let needs_port = is_group || port != 80;
    if !needs_port {
        return host.to_string();
    }
    if host.contains(':') {
        format!("[{}]:{}", host, port)
    } else {
        format!("{}:{}", host, port)
    }
}

/// 追加客户端原始头，并跳过已覆盖或连接级头。
fn append_origin_headers(
    out: &mut Vec<u8>,
    req: &Request,
    proxy: &ProxyConfig,
    set_names: &HashSet<String>,
    websocket: bool,
) {
    if !proxy.pass_request_headers {
        return;
    }
    let conn_tokens = http::connection_tokens(&req.head.headers);
    for h in &req.head.headers {
        let lname = h.name.to_ascii_lowercase();
        if SKIP_REQUEST_HEADERS.contains(&lname.as_str())
            || (websocket && SKIP_WS_HEADERS.contains(&lname.as_str()))
            || set_names.contains(&lname)
            || conn_tokens.iter().any(|t| t == &lname)
        {
            continue;
        }
        push_header(out, &h.name, &h.value);
    }
}

/// 向请求缓冲区写入一个经过清洗的头。
fn push_header(out: &mut Vec<u8>, name: &str, value: &str) {
    out.extend_from_slice(name.as_bytes());
    out.extend_from_slice(b": ");
    out.extend_from_slice(http::sanitize_header_value(value).as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// 变量展开上下文。
struct VarCtx<'a> {
    req: &'a Request,
    remote_ip: IpAddr,
    upstream_port: u16,
    host_header: &'a str,
    server: &'a ServerConfig,
    local_port: u16,
}

/// 展开 proxy_set_header 中的变量。
fn expand_vars(template: &str, ctx: &VarCtx) -> String {
    let chars: Vec<char> = template.chars().collect();
    let mut out = String::with_capacity(template.len());
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] != '$' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        if i + 1 < chars.len() && chars[i + 1] == '$' {
            out.push('$');
            i += 2;
            continue;
        }
        let (name, next) = if i + 1 < chars.len() && chars[i + 1] == '{' {
            let mut j = i + 2;
            while j < chars.len() && chars[j] != '}' {
                j += 1;
            }
            if j < chars.len() {
                (chars[i + 2..j].iter().collect::<String>(), j + 1)
            } else {
                (String::new(), chars.len())
            }
        } else {
            let mut j = i + 1;
            while j < chars.len() && (chars[j].is_ascii_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            (chars[i + 1..j].iter().collect::<String>(), j)
        };
        if name.is_empty() {
            out.push('$');
            i += 1;
            continue;
        }
        out.push_str(&lookup_var(&name.to_ascii_lowercase(), ctx));
        i = next;
    }
    out
}

/// 解析单个变量的值。
fn lookup_var(name: &str, ctx: &VarCtx) -> String {
    match name {
        "host" => ctx
            .req
            .header("host")
            .map(host_without_port)
            .unwrap_or_default(),
        "proxy_host" => ctx.host_header.to_string(),
        "proxy_port" => ctx.upstream_port.to_string(),
        "remote_addr" => ctx.remote_ip.to_string(),
        "proxy_add_x_forwarded_for" => {
            let existing = ctx.req.header_all("x-forwarded-for").join(", ");
            if existing.is_empty() {
                ctx.remote_ip.to_string()
            } else {
                format!("{}, {}", existing, ctx.remote_ip)
            }
        }
        "request_uri" => ctx.req.head.target.clone(),
        "scheme" => "http".to_string(),
        "server_name" => ctx.server.server_name.clone().unwrap_or_default(),
        "server_port" => ctx.local_port.to_string(),
        _ => {
            if let Some(header_name) = name.strip_prefix("http_") {
                let header_name = header_name.replace('_', "-");
                let values = ctx.req.header_all(&header_name);
                values.join(", ")
            } else {
                String::new()
            }
        }
    }
}

/// 去掉 Host 头中的端口。
fn host_without_port(host: &str) -> String {
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

/// 带超时地从上游读取数据。
async fn read_upstream<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
    timeout: Duration,
) -> Result<usize, HttpError> {
    match tokio::time::timeout(timeout, reader.read(buf)).await {
        Ok(Ok(n)) => Ok(n),
        Ok(Err(e)) => Err(HttpError::upstream(format!("upstream read failed: {}", e))),
        Err(_) => Err(HttpError::upstream(format!(
            "upstream read timed out after {:?}",
            timeout
        ))),
    }
}

/// 带超时地从上游读取固定长度数据。
async fn read_exact_upstream<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
    timeout: Duration,
) -> Result<(), HttpError> {
    match tokio::time::timeout(timeout, reader.read_exact(buf)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(HttpError::upstream(format!("upstream read failed: {}", e))),
        Err(_) => Err(HttpError::upstream(format!(
            "upstream read timed out after {:?}",
            timeout
        ))),
    }
}

/// 带超时地向上游写入数据。
async fn write_upstream<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
    timeout: Duration,
) -> Result<(), HttpError> {
    match tokio::time::timeout(timeout, writer.write_all(data)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(HttpError::upstream(format!("upstream write failed: {}", e))),
        Err(_) => Err(HttpError::upstream(format!(
            "upstream write timed out after {:?}",
            timeout
        ))),
    }
}

/// 带超时地向客户端写入数据。
async fn write_client<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
    timeout: Duration,
) -> Result<(), HttpError> {
    match tokio::time::timeout(timeout, writer.write_all(data)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(HttpError::new(499, format!("client write failed: {}", e))),
        Err(_) => Err(HttpError::new(499, "client write timed out")),
    }
}

/// 带超时地刷新客户端输出。
async fn flush_client<W: AsyncWrite + Unpin>(
    writer: &mut W,
    timeout: Duration,
) -> Result<(), HttpError> {
    match tokio::time::timeout(timeout, writer.flush()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(HttpError::new(499, format!("client flush failed: {}", e))),
        Err(_) => Err(HttpError::new(499, "client flush timed out")),
    }
}

/// 按 Content-Length 精确转发指定字节数。
async fn relay_exact<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    mut remaining: u64,
    read_timeout: Duration,
    write_timeout: Duration,
) -> Result<(), HttpError> {
    let mut buf = vec![0u8; RELAY_BUF];
    while remaining > 0 {
        let want = remaining.min(RELAY_BUF as u64) as usize;
        let n = read_upstream(reader, &mut buf[..want], read_timeout).await?;
        if n == 0 {
            return Err(HttpError::upstream(
                "upstream closed before the response body was complete",
            ));
        }
        write_client(writer, &buf[..n], write_timeout).await?;
        remaining -= n as u64;
    }
    Ok(())
}

/// 一直转发到上游关闭。
async fn relay_until_eof<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    read_timeout: Duration,
    write_timeout: Duration,
) -> Result<(), HttpError> {
    let mut buf = vec![0u8; RELAY_BUF];
    loop {
        let n = read_upstream(reader, &mut buf, read_timeout).await?;
        if n == 0 {
            return Ok(());
        }
        write_client(writer, &buf[..n], write_timeout).await?;
    }
}

/// 原样中继 chunked 响应体。
async fn relay_chunked<R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    read_timeout: Duration,
    write_timeout: Duration,
) -> Result<(), HttpError> {
    loop {
        let line = read_wire_line_timed(reader, read_timeout).await?;
        write_client(writer, &line, write_timeout).await?;

        let stripped = strip_eol_bytes(&line);
        let text = String::from_utf8_lossy(stripped);
        let size_text = text.split(';').next().unwrap_or("").trim();
        let size = u64::from_str_radix(size_text, 16)
            .map_err(|_| HttpError::upstream("upstream sent an invalid chunk size"))?;

        if size == 0 {
            loop {
                let line = read_wire_line_timed(reader, read_timeout).await?;
                write_client(writer, &line, write_timeout).await?;
                if is_empty_line(&line) {
                    return Ok(());
                }
            }
        }

        relay_exact(reader, writer, size, read_timeout, write_timeout).await?;
        let mut crlf = [0u8; 2];
        read_exact_upstream(reader, &mut crlf, read_timeout).await?;
        if &crlf != b"\r\n" {
            return Err(HttpError::upstream(
                "upstream chunk data is not followed by CRLF",
            ));
        }
        write_client(writer, &crlf, write_timeout).await?;
    }
}

/// 带超时地读取一行 chunked 协议数据。
async fn read_wire_line_timed<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    timeout: Duration,
) -> Result<Vec<u8>, HttpError> {
    match tokio::time::timeout(timeout, http::read_wire_line(reader)).await {
        Ok(Ok(Some(line))) => Ok(line),
        Ok(Ok(None)) => Err(HttpError::upstream(
            "upstream closed in the middle of a chunked response",
        )),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(HttpError::upstream(format!(
            "upstream read timed out after {:?}",
            timeout
        ))),
    }
}

/// 去掉字节行末尾的 CR/LF。
fn strip_eol_bytes(mut line: &[u8]) -> &[u8] {
    if line.last() == Some(&b'\n') {
        line = &line[..line.len() - 1];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
    }
    line
}

/// 判断是否为空行。
fn is_empty_line(line: &[u8]) -> bool {
    line == b"\r\n" || line == b"\n"
}

/// 完成 WebSocket 握手并进入双向裸转发。
#[allow(clippy::too_many_arguments)]
pub async fn handle_websocket(
    mut client: BufReader<TcpStream>,
    app: &App,
    req: &Request,
    proxy: &ProxyConfig,
    remote_ip: IpAddr,
    local_port: u16,
    server: &ServerConfig,
) -> std::io::Result<()> {
    let group = resolve_group(app, proxy);
    let mut excluded: Vec<usize> = Vec::new();
    let mut direct_used = false;
    let mut last_error = HttpError::upstream("no available upstream peer");

    loop {
        let cand = if let Some(g) = group {
            match g.select(&excluded) {
                Some(idx) => {
                    excluded.push(idx);
                    Some(idx)
                }
                None => break,
            }
        } else {
            if direct_used {
                break;
            }
            direct_used = true;
            None
        };

        let (host, port) = match cand {
            Some(idx) => {
                let g = group.expect("peer index without group");
                let (h, p) = g.address(idx);
                (h.to_string(), p)
            }
            None => (proxy.target_host.clone(), proxy.target_port),
        };
        let is_group = group.is_some();
        let request_bytes = build_ws_request(
            req, proxy, &host, port, is_group, remote_ip, local_port, server,
        );

        let mut conn =
            match open_connection(app, group, &host, port, proxy.connect_timeout, false).await {
                Ok(c) => c,
                Err(e) => {
                    if let (Some(g), Some(idx)) = (group, cand) {
                        g.mark_failure(idx);
                    }
                    log::warn(format!(
                        "ws: connect to {}:{} failed: {}",
                        host, port, e.message
                    ));
                    last_error = e;
                    continue;
                }
            };

        if let Err(e) =
            write_upstream(conn.reader.get_mut(), &request_bytes, proxy.send_timeout).await
        {
            if let (Some(g), Some(idx)) = (group, cand) {
                g.mark_failure(idx);
            }
            last_error = e;
            continue;
        }

        let raw = match tokio::time::timeout(
            proxy.read_timeout,
            http::read_raw_response_head(&mut conn.reader),
        )
        .await
        {
            Ok(Ok(raw)) => raw,
            Ok(Err(e)) => {
                if let (Some(g), Some(idx)) = (group, cand) {
                    g.mark_failure(idx);
                }
                last_error = e;
                continue;
            }
            Err(_) => {
                if let (Some(g), Some(idx)) = (group, cand) {
                    g.mark_failure(idx);
                }
                last_error = HttpError::new(
                    502,
                    format!(
                        "ws handshake from {}:{} timed out after {:?}",
                        host, port, proxy.read_timeout
                    ),
                );
                continue;
            }
        };

        let head = match http::parse_response_head(&raw) {
            Ok(h) => h,
            Err(e) => {
                if let (Some(g), Some(idx)) = (group, cand) {
                    g.mark_failure(idx);
                }
                last_error = e;
                continue;
            }
        };

        if head.status == 101 {
            write_client(client.get_mut(), &raw, proxy.send_timeout)
                .await
                .map_err(http_error_to_io)?;
            flush_client(client.get_mut(), proxy.send_timeout)
                .await
                .map_err(http_error_to_io)?;

            let client_leftover = client.buffer().to_vec();
            let upstream_leftover = conn.reader.buffer().to_vec();
            let mut client_stream = client.into_inner();
            let mut upstream_stream = conn.reader.into_inner();

            if !client_leftover.is_empty() {
                write_upstream(&mut upstream_stream, &client_leftover, proxy.send_timeout)
                    .await
                    .map_err(http_error_to_io)?;
            }
            if !upstream_leftover.is_empty() {
                write_client(&mut client_stream, &upstream_leftover, proxy.send_timeout)
                    .await
                    .map_err(http_error_to_io)?;
                flush_client(&mut client_stream, proxy.send_timeout)
                    .await
                    .map_err(http_error_to_io)?;
            }

            if let (Some(g), Some(idx)) = (group, cand) {
                g.mark_success(idx);
            }
            log::info("ws: upgrade complete, entering bidirectional relay");
            let _ = tokio::io::copy_bidirectional(&mut client_stream, &mut upstream_stream).await;
            log::info("ws: relay closed");
            return Ok(());
        }

        let framing = match http::response_framing(&req.head.method, &head) {
            Ok(f) => f,
            Err(e) => {
                last_error = e;
                continue;
            }
        };
        if let Err(e) = forward_response(
            client.get_mut(),
            req,
            &head,
            &mut conn.reader,
            framing,
            proxy,
            false,
        )
        .await
        {
            log::debug(format!("ws: fallback response relay failed: {}", e.message));
            if e.status != 499 {
                send_error(client.get_mut(), req, &e).await;
            }
            return Ok(());
        }
        return Ok(());
    }

    send_error(client.get_mut(), req, &last_error).await;
    Ok(())
}

/// 构造 WebSocket 升级请求。
#[allow(clippy::too_many_arguments)]
fn build_ws_request(
    req: &Request,
    proxy: &ProxyConfig,
    upstream_host: &str,
    upstream_port: u16,
    is_group: bool,
    remote_ip: IpAddr,
    local_port: u16,
    server: &ServerConfig,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(1024);
    let path = build_target_path(proxy, req);
    let host_header = build_host_header(upstream_host, upstream_port, is_group);
    let ctx = VarCtx {
        req,
        remote_ip,
        upstream_port,
        host_header: &host_header,
        server,
        local_port,
    };
    let set_names: HashSet<String> = proxy
        .set_headers
        .iter()
        .map(|(k, _)| k.to_ascii_lowercase())
        .collect();

    out.extend_from_slice(req.head.method.as_bytes());
    out.push(b' ');
    out.extend_from_slice(path.as_bytes());
    out.extend_from_slice(b" HTTP/1.1\r\n");

    push_header(&mut out, "Host", &host_header);

    if req.header("x-real-ip").is_none() && !set_names.contains("x-real-ip") {
        push_header(&mut out, "X-Real-IP", &remote_ip.to_string());
    }

    let existing_xff = req.header_all("x-forwarded-for").join(", ");
    if !set_names.contains("x-forwarded-for") {
        let xff = if existing_xff.is_empty() {
            remote_ip.to_string()
        } else {
            format!("{}, {}", existing_xff, remote_ip)
        };
        push_header(&mut out, "X-Forwarded-For", &xff);
    }

    for (key, value) in &proxy.set_headers {
        push_header(&mut out, key, &expand_vars(value, &ctx));
    }

    if let Some(upgrade) = req.header("upgrade") {
        push_header(&mut out, "Upgrade", upgrade);
    } else {
        push_header(&mut out, "Upgrade", "websocket");
    }
    out.extend_from_slice(b"Connection: Upgrade\r\n");

    if let Some(key) = req.header("sec-websocket-key") {
        push_header(&mut out, "Sec-WebSocket-Key", key);
    }
    if let Some(version) = req.header("sec-websocket-version") {
        push_header(&mut out, "Sec-WebSocket-Version", version);
    }
    for protocol in req.header_all("sec-websocket-protocol") {
        push_header(&mut out, "Sec-WebSocket-Protocol", protocol);
    }
    for extensions in req.header_all("sec-websocket-extensions") {
        push_header(&mut out, "Sec-WebSocket-Extensions", extensions);
    }

    append_origin_headers(&mut out, req, proxy, &set_names, true);

    out.extend_from_slice(b"\r\n");
    out
}
