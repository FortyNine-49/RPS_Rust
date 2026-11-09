//! HTTP/1.x 工具：请求/响应解析、头部校验、body framing、错误与默认响应序列化。
use std::fmt;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// HTTP 头部大小上限。
pub const MAX_HEAD_SIZE: usize = 64 * 1024;
// 单个消息的头部数量上限。
pub const MAX_HEADERS: usize = 200;

// 未配置时请求体的默认上限（64 MiB）。
pub const DEFAULT_BODY_MAX: u64 = 64 * 1024 * 1024;

/// HTTP 版本。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Version {
    Http10,
    Http11,
}

/// HTTP 版本相关方法。
impl Version {
    /// 返回 HTTP/1.0 或 HTTP/1.1 字符串。
    pub fn as_str(self) -> &'static str {
        match self {
            Version::Http10 => "HTTP/1.0",
            Version::Http11 => "HTTP/1.1",
        }
    }
}

/// 一个 HTTP 头键值对。
#[derive(Clone, Debug)]
pub struct Header {
    pub name: String,
    pub value: String,
}

/// 请求头解析结果：方法、路径、版本和 body framing 信息。
#[derive(Clone, Debug)]
pub struct RequestHead {
    pub method: String,
    pub target: String,
    pub path: String,
    pub query: String,
    pub version: Version,
    pub headers: Vec<Header>,
    pub keepalive: bool,
    pub content_length: Option<u64>,
    pub chunked: bool,
    pub expect_continue: bool,
}

/// 请求头查询辅助。
impl RequestHead {
    /// 按名称取第一个头值。
    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }

    /// 按名称取所有同名头值。
    pub fn header_all(&self, name: &str) -> Vec<&str> {
        header_all(&self.headers, name)
    }

    /// 判断指定头是否包含某个逗号分隔的 token。
    pub fn header_contains_token(&self, name: &str, token: &str) -> bool {
        self.header_all(name)
            .iter()
            .any(|v| header_contains_token(v, token))
    }
}

/// 完整请求：请求头加已读取的 body。
#[derive(Clone, Debug)]
pub struct Request {
    pub head: RequestHead,
    pub body: Vec<u8>,
}

/// 请求头查询辅助。
impl Request {
    /// 按名称取第一个头值。
    pub fn header(&self, name: &str) -> Option<&str> {
        self.head.header(name)
    }

    /// 按名称取所有同名头值。
    pub fn header_all(&self, name: &str) -> Vec<&str> {
        self.head.header_all(name)
    }

    /// 判断指定头是否包含某个 token。
    pub fn header_contains_token(&self, name: &str, token: &str) -> bool {
        self.head.header_contains_token(name, token)
    }
}

/// 后端响应头解析结果。
#[derive(Clone, Debug)]
pub struct ResponseHead {
    pub version: Version,
    pub status: u16,
    pub reason: String,
    pub headers: Vec<Header>,
}

/// 响应头查询辅助。
impl ResponseHead {
    /// 按名称取第一个响应头值。
    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }

    /// 按名称取所有同名响应头值。
    pub fn header_all(&self, name: &str) -> Vec<&str> {
        header_all(&self.headers, name)
    }
}

/// 响应 body 的 framing 方式。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyFraming {
    None,
    Length(u64),
    Chunked,
    UntilEof,
}

/// 带 HTTP 状态码的错误。
#[derive(Debug)]
pub struct HttpError {
    pub status: u16,
    pub message: String,
}

/// HttpError 构造辅助。
impl HttpError {
    /// 创建指定状态码和消息的错误。
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    /// 创建 400 错误。
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(400, message)
    }

    /// 创建 413 错误。
    pub fn too_large(message: impl Into<String>) -> Self {
        Self::new(413, message)
    }

    /// 创建 502 上游错误。
    pub fn upstream(message: impl Into<String>) -> Self {
        Self::new(502, message)
    }
}

/// 输出状态码和消息。
impl fmt::Display for HttpError {
    /// 格式化错误文本。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.status, self.message)
    }
}

/// 让 HttpError 可作为标准错误使用。
impl std::error::Error for HttpError {}

/// 在头部数组中按名称查找第一个值。
pub fn header<'a>(headers: &'a [Header], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
}

/// 在头部数组中查找所有同名值。
pub fn header_all<'a>(headers: &'a [Header], name: &str) -> Vec<&'a str> {
    headers
        .iter()
        .filter(|h| h.name.eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
        .collect()
}

/// 判断头部值是否包含指定 token。
pub fn header_contains_token(value: &str, token: &str) -> bool {
    value.split(',').any(|part| {
        part.split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case(token)
    })
}

/// 判断是否为 hop-by-hop 头。
pub fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-connection"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

/// 收集 Connection 头中列出的 hop-by-hop 头名。
pub fn connection_tokens(headers: &[Header]) -> Vec<String> {
    let mut out = Vec::new();
    for v in header_all(headers, "connection") {
        for part in v.split(',') {
            let name = part.trim();
            if !name.is_empty() {
                out.push(name.to_ascii_lowercase());
            }
        }
    }
    out
}

/// 判断字符串是否符合 HTTP token 规则。
pub fn is_token(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

/// 判断字符串是否含控制字符。
fn has_ctl(s: &str) -> bool {
    s.chars().any(|c| (c < ' ' && c != '\t') || c == '\x7f')
}

/// 去除头值中的 CR/LF/NUL，防止头注入。
pub fn sanitize_header_value(v: &str) -> String {
    v.chars()
        .filter(|c| *c != '\r' && *c != '\n' && *c != '\0')
        .collect()
}

/// 返回常见状态码的原因短语。
pub fn reason_phrase(status: u16) -> &'static str {
    match status {
        100 => "Continue",
        101 => "Switching Protocols",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        411 => "Length Required",
        413 => "Payload Too Large",
        414 => "URI Too Long",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        505 => "HTTP Version Not Supported",
        _ => "",
    }
}

/// 读取一行原始字节，并限制整个头块大小。
async fn read_raw_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    total: &mut usize,
    err_status: u16,
) -> Result<Option<Vec<u8>>, HttpError> {
    let mut out = Vec::new();
    loop {
        let mut chunk = Vec::new();
        let mut found = false;
        let mut eof = false;
        {
            let available = reader
                .fill_buf()
                .await
                .map_err(|e| HttpError::new(err_status, format!("read error: {}", e)))?;
            if available.is_empty() {
                eof = true;
            } else if let Some(pos) = available.iter().position(|&b| b == b'\n') {
                chunk.extend_from_slice(&available[..=pos]);
                found = true;
            } else {
                chunk.extend_from_slice(available);
            }
        }
        if eof {
            if out.is_empty() {
                return Ok(None);
            }
            return Ok(Some(out));
        }
        let n = chunk.len();
        reader.consume(n);
        *total = total.saturating_add(n);
        if *total > MAX_HEAD_SIZE {
            let status = if err_status == 400 { 431 } else { err_status };
            return Err(HttpError::new(
                status,
                "header block exceeds the size limit",
            ));
        }
        out.extend_from_slice(&chunk);
        if found {
            return Ok(Some(out));
        }
    }
}

/// 读取一行原始字节，供 chunked 解析使用。
pub async fn read_wire_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Vec<u8>>, HttpError> {
    let mut total = 0usize;
    read_raw_line(reader, &mut total, 502).await
}

/// 去掉行尾的 CR/LF。
fn strip_eol(mut line: Vec<u8>) -> Vec<u8> {
    if line.last() == Some(&b'\n') {
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
    }
    line
}

/// 把原始行转换为字符串并去掉行尾。
fn line_string(line: Vec<u8>) -> String {
    String::from_utf8_lossy(&strip_eol(line)).into_owned()
}

/// 读取并校验请求行与请求头。
pub async fn read_request_head<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<RequestHead>, HttpError> {
    let mut total = 0usize;
    let mut request_line = None;
    while request_line.is_none() {
        match read_raw_line(reader, &mut total, 400).await? {
            None => return Ok(None),
            Some(raw) => {
                let line = line_string(raw);
                if line.is_empty() {
                    continue;
                }
                request_line = Some(line);
            }
        }
    }
    let request_line = request_line.unwrap();

    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| HttpError::bad_request("missing method"))?;
    let target = parts
        .next()
        .ok_or_else(|| HttpError::bad_request("missing request target"))?;
    let version_str = parts
        .next()
        .ok_or_else(|| HttpError::bad_request("missing HTTP version"))?;
    if parts.next().is_some() {
        return Err(HttpError::bad_request("request line has too many parts"));
    }
    if !is_token(method) {
        return Err(HttpError::bad_request("invalid method"));
    }
    if target.chars().any(|c| c.is_control()) {
        return Err(HttpError::bad_request(
            "invalid control character in request target",
        ));
    }
    let version = match version_str {
        "HTTP/1.0" => Version::Http10,
        "HTTP/1.1" => Version::Http11,
        _ => {
            return Err(HttpError::new(
                505,
                format!("unsupported HTTP version \"{}\"", version_str),
            ))
        }
    };

    let (path, query) = split_target(target)?;

    let mut headers: Vec<Header> = Vec::new();
    loop {
        let raw = read_raw_line(reader, &mut total, 400)
            .await?
            .ok_or_else(|| HttpError::bad_request("unexpected EOF in request headers"))?;
        let line = line_string(raw);
        if line.is_empty() {
            break;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            return Err(HttpError::bad_request(
                "obsolete header folding is not supported",
            ));
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| HttpError::bad_request("malformed header line"))?;
        let name = name.trim();
        if !is_token(name) {
            return Err(HttpError::bad_request("invalid header name"));
        }
        let value = value.trim_matches(|c| c == ' ' || c == '\t');
        if has_ctl(value) {
            return Err(HttpError::bad_request(
                "invalid control character in header value",
            ));
        }
        headers.push(Header {
            name: name.to_string(),
            value: value.to_string(),
        });
        if headers.len() > MAX_HEADERS {
            return Err(HttpError::new(431, "too many request headers"));
        }
    }

    let connection_close = header_all(&headers, "connection")
        .iter()
        .any(|v| header_contains_token(v, "close"));
    let connection_keepalive = header_all(&headers, "connection")
        .iter()
        .any(|v| header_contains_token(v, "keep-alive"));
    let keepalive = match version {
        Version::Http11 => !connection_close,
        Version::Http10 => connection_keepalive && !connection_close,
    };

    let (content_length, chunked) = parse_body_headers(&headers)?;
    let expect_continue = header_all(&headers, "expect")
        .iter()
        .any(|v| header_contains_token(v, "100-continue"))
        && (chunked || content_length.unwrap_or(0) > 0);

    Ok(Some(RequestHead {
        method: method.to_string(),
        target: target.to_string(),
        path,
        query,
        version,
        headers,
        keepalive,
        content_length,
        chunked,
        expect_continue,
    }))
}

/// 按 Content-Length 或 chunked 读取请求体。
pub async fn read_request_body<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    head: &RequestHead,
    max_size: u64,
) -> Result<Vec<u8>, HttpError> {
    if head.chunked {
        read_chunked_body(reader, max_size).await
    } else if let Some(len) = head.content_length {
        if len > max_size {
            return Err(HttpError::too_large(format!(
                "request body is {} bytes, limit is {}",
                len, max_size
            )));
        }
        let len_usize = usize::try_from(len)
            .map_err(|_| HttpError::too_large("request body is too large for this platform"))?;
        let mut body = vec![0u8; len_usize];
        reader
            .read_exact(&mut body)
            .await
            .map_err(|e| HttpError::bad_request(format!("incomplete request body: {}", e)))?;
        Ok(body)
    } else {
        Ok(Vec::new())
    }
}

/// 解码 chunked 请求体并检查大小上限。
async fn read_chunked_body<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_size: u64,
) -> Result<Vec<u8>, HttpError> {
    let mut body = Vec::new();
    let mut total = 0usize;
    loop {
        let mut line_total = 0usize;
        let raw = read_raw_line(reader, &mut line_total, 400)
            .await?
            .ok_or_else(|| HttpError::bad_request("unexpected EOF in chunked body"))?;
        let line = line_string(raw);
        let size_text = line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|_| HttpError::bad_request("invalid chunk size"))?;
        if size == 0 {
            loop {
                let raw = read_raw_line(reader, &mut line_total, 400)
                    .await?
                    .ok_or_else(|| HttpError::bad_request("unexpected EOF in chunk trailers"))?;
                let line = line_string(raw);
                if line.is_empty() {
                    break;
                }
            }
            break;
        }
        total = total.saturating_add(size);
        if total as u64 > max_size {
            return Err(HttpError::too_large(
                "chunked request body exceeds the limit",
            ));
        }
        let start = body.len();
        body.resize(start + size, 0);
        reader
            .read_exact(&mut body[start..])
            .await
            .map_err(|e| HttpError::bad_request(format!("incomplete chunk data: {}", e)))?;
        let mut crlf = [0u8; 2];
        reader
            .read_exact(&mut crlf)
            .await
            .map_err(|e| HttpError::bad_request(format!("incomplete chunk terminator: {}", e)))?;
        if &crlf != b"\r\n" {
            return Err(HttpError::bad_request("chunk data is not followed by CRLF"));
        }
    }
    Ok(body)
}

/// 读取完整请求（头和 body），供测试使用。
#[allow(dead_code)]
pub async fn read_request<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_body_size: u64,
) -> Result<Option<Request>, HttpError> {
    let head = match read_request_head(reader).await? {
        Some(h) => h,
        None => return Ok(None),
    };
    let body = read_request_body(reader, &head, max_body_size).await?;
    Ok(Some(Request { head, body }))
}

/// 拆分请求目标中的路径和 query。
fn split_target(target: &str) -> Result<(String, String), HttpError> {
    let mut rest = target;
    if let Some(idx) = target.find("://") {
        let after = &target[idx + 3..];
        rest = match after.find('/') {
            Some(i) => &after[i..],
            None => "/",
        };
    }
    if !rest.starts_with('/') && rest != "*" {
        return Err(HttpError::bad_request(
            "request target must be origin-form or absolute-form",
        ));
    }
    let (path, query) = match rest.split_once('?') {
        Some((p, q)) => (p, q),
        None => (rest, ""),
    };
    let path = if path.is_empty() { "/" } else { path };
    Ok((path.to_string(), query.to_string()))
}

/// 校验 Content-Length 和 Transfer-Encoding，确定 body 读取方式。
fn parse_body_headers(headers: &[Header]) -> Result<(Option<u64>, bool), HttpError> {
    let te_values = header_all(headers, "transfer-encoding");
    let chunked = !te_values.is_empty();
    if chunked {
        if header(headers, "content-length").is_some() {
            return Err(HttpError::bad_request(
                "both Content-Length and Transfer-Encoding are present",
            ));
        }
        let joined = te_values.join(",");
        let tokens: Vec<&str> = joined
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        if tokens.last().map(|t| t.eq_ignore_ascii_case("chunked")) != Some(true) {
            return Err(HttpError::new(
                501,
                "only chunked transfer-encoding is supported",
            ));
        }
        if tokens.len() != 1 {
            return Err(HttpError::new(
                501,
                "transfer-encoding codings other than chunked are not supported",
            ));
        }
        return Ok((None, true));
    }

    let values = header_all(headers, "content-length");
    if values.is_empty() {
        return Ok((None, false));
    }
    let mut parsed: Option<u64> = None;
    for value in values {
        for part in value.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let n = part
                .parse::<u64>()
                .map_err(|_| HttpError::bad_request("invalid Content-Length"))?;
            match parsed {
                Some(prev) if prev != n => {
                    return Err(HttpError::bad_request("conflicting Content-Length values"))
                }
                _ => parsed = Some(n),
            }
        }
    }
    Ok((parsed, false))
}

/// 读取后端响应头原始字节。
pub async fn read_raw_response_head<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Vec<u8>, HttpError> {
    let mut raw = Vec::new();
    let mut total = 0usize;
    loop {
        let line = read_raw_line(reader, &mut total, 502)
            .await?
            .ok_or_else(|| HttpError::upstream("unexpected EOF while reading response headers"))?;
        let empty = line == b"\r\n" || line == b"\n";
        raw.extend_from_slice(&line);
        if empty {
            return Ok(raw);
        }
    }
}

/// 解析后端状态行和响应头。
pub fn parse_response_head(raw: &[u8]) -> Result<ResponseHead, HttpError> {
    let text = String::from_utf8_lossy(raw);
    let mut lines = text.split('\n');
    let status_line = lines
        .next()
        .ok_or_else(|| HttpError::upstream("empty response"))?
        .trim_end_matches('\r');
    let mut parts = status_line.splitn(3, ' ');
    let version_str = parts
        .next()
        .ok_or_else(|| HttpError::upstream("malformed status line"))?;
    let version = match version_str {
        "HTTP/1.0" => Version::Http10,
        "HTTP/1.1" => Version::Http11,
        other => {
            return Err(HttpError::upstream(format!(
                "upstream sent unsupported version \"{}\"",
                other
            )))
        }
    };
    let status = parts
        .next()
        .ok_or_else(|| HttpError::upstream("missing status code"))?
        .parse::<u16>()
        .map_err(|_| HttpError::upstream("invalid status code"))?;
    let reason = parts.next().unwrap_or("").trim().to_string();
    let reason = if reason.is_empty() {
        reason_phrase(status).to_string()
    } else {
        sanitize_header_value(&reason)
    };

    let mut headers = Vec::new();
    for line in lines {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| HttpError::upstream("malformed response header"))?;
        let name = name.trim();
        if !is_token(name) {
            return Err(HttpError::upstream("invalid response header name"));
        }
        let value = value.trim_matches(|c| c == ' ' || c == '\t');
        if has_ctl(value) {
            return Err(HttpError::upstream(
                "invalid control character in response header",
            ));
        }
        headers.push(Header {
            name: name.to_string(),
            value: value.to_string(),
        });
        if headers.len() > MAX_HEADERS {
            return Err(HttpError::upstream("too many response headers"));
        }
    }

    Ok(ResponseHead {
        version,
        status,
        reason,
        headers,
    })
}

/// 读取最终响应头，跳过 1xx 临时响应。
pub async fn read_response_head<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<ResponseHead, HttpError> {
    loop {
        let raw = read_raw_response_head(reader).await?;
        let head = parse_response_head(&raw)?;
        if (100..200).contains(&head.status) && head.status != 101 {
            continue;
        }
        return Ok(head);
    }
}

/// 根据方法和响应头确定 body 的 framing 方式。
pub fn response_framing(method: &str, head: &ResponseHead) -> Result<BodyFraming, HttpError> {
    if method.eq_ignore_ascii_case("HEAD")
        || head.status == 101
        || head.status == 204
        || head.status == 304
        || (100..200).contains(&head.status)
    {
        return Ok(BodyFraming::None);
    }

    let te_values = header_all(&head.headers, "transfer-encoding");
    if !te_values.is_empty() {
        let joined = te_values.join(",");
        let tokens: Vec<&str> = joined
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        if tokens.last().map(|t| t.eq_ignore_ascii_case("chunked")) != Some(true)
            || tokens.len() != 1
        {
            return Err(HttpError::upstream(
                "upstream used an unsupported transfer-encoding",
            ));
        }
        return Ok(BodyFraming::Chunked);
    }

    if let Some(cl) = head.header("content-length") {
        let n = cl
            .trim()
            .parse::<u64>()
            .map_err(|_| HttpError::upstream("invalid upstream Content-Length"))?;
        return Ok(BodyFraming::Length(n));
    }

    Ok(BodyFraming::UntilEof)
}

/// 判断后端连接是否可继续复用。
#[allow(dead_code)]
pub fn response_keepalive(head: &ResponseHead, framing: BodyFraming) -> bool {
    if framing == BodyFraming::UntilEof || head.status == 101 {
        return false;
    }
    let close = head
        .header_all("connection")
        .iter()
        .any(|v| header_contains_token(v, "close"));
    if close {
        return false;
    }
    match head.version {
        Version::Http11 => true,
        Version::Http10 => head
            .header_all("connection")
            .iter()
            .any(|v| header_contains_token(v, "keep-alive")),
    }
}

/// 写出状态行、固定头和 body 的简单响应。
pub async fn write_simple_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    version: Version,
    status: u16,
    reason: &str,
    body: &[u8],
    keepalive: bool,
    extra_headers: &[(&str, &str)],
) -> std::io::Result<()> {
    let mut head = Vec::with_capacity(256);
    head.extend_from_slice(version.as_str().as_bytes());
    let reason = if reason.is_empty() {
        reason_phrase(status)
    } else {
        reason
    };
    head.extend_from_slice(format!(" {} {}\r\n", status, reason).as_bytes());
    head.extend_from_slice(b"Server: RPS\r\n");
    head.extend_from_slice(b"Content-Type: text/html\r\n");
    head.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
    for (name, value) in extra_headers {
        head.extend_from_slice(
            format!("{}: {}\r\n", name, sanitize_header_value(value)).as_bytes(),
        );
    }
    if keepalive {
        head.extend_from_slice(b"Connection: keep-alive\r\nKeep-Alive: timeout=60\r\n");
    } else {
        head.extend_from_slice(b"Connection: close\r\n");
    }
    head.extend_from_slice(b"\r\n");
    head.extend_from_slice(body);
    writer.write_all(&head).await?;
    writer.flush().await
}

// HTTP 解析与 framing 单元测试。
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    /// 测试辅助：解析请求头。
    async fn head(input: &[u8]) -> RequestHead {
        let mut reader = BufReader::new(input);
        read_request_head(&mut reader).await.unwrap().unwrap()
    }

    /// 验证基本 GET 请求解析。
    #[tokio::test]
    async fn parses_simple_get() {
        let h = head(b"GET /hello?x=1 HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
            .await;
        assert_eq!(h.method, "GET");
        assert_eq!(h.path, "/hello");
        assert_eq!(h.query, "x=1");
        assert_eq!(h.version, Version::Http11);
        assert!(!h.keepalive);
        assert_eq!(h.header("host"), Some("example.com"));
    }

    /// 验证 HTTP/1.0 的 keepalive 规则。
    #[tokio::test]
    async fn http10_keepalive_requires_header() {
        let h = head(b"GET / HTTP/1.0\r\n\r\n").await;
        assert!(!h.keepalive);
        let h = head(b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n").await;
        assert!(h.keepalive);
    }

    /// 验证 Content-Length body 读取。
    #[tokio::test]
    async fn reads_content_length_body() {
        let mut reader = BufReader::new(&b"POST /x HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello"[..]);
        let req = read_request(&mut reader, 1024).await.unwrap().unwrap();
        assert_eq!(req.body, b"hello");
    }

    /// 验证 chunked body 解码。
    #[tokio::test]
    async fn decodes_chunked_body() {
        let input =
            b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let mut reader = BufReader::new(&input[..]);
        let req = read_request(&mut reader, 1024).await.unwrap().unwrap();
        assert_eq!(req.body, b"hello world");
    }

    /// 验证响应头解析和 framing 判断。
    #[tokio::test]
    async fn parses_response_and_framing() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\n";
        let head = parse_response_head(raw).unwrap();
        assert_eq!(head.status, 200);
        assert_eq!(
            response_framing("GET", &head).unwrap(),
            BodyFraming::Length(5)
        );
        assert!(response_keepalive(&head, BodyFraming::Length(5)));
    }
}
