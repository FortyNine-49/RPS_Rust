//! HTTP/1.x message types, parsing and serialization.

use std::fmt;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_HEAD_SIZE: usize = 64 * 1024;
pub const MAX_HEADERS: usize = 200;
/// Absolute cap used when `http_body_max_size` is not configured.
pub const DEFAULT_BODY_MAX: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Version {
    Http10,
    Http11,
}

impl Version {
    pub fn as_str(self) -> &'static str {
        match self {
            Version::Http10 => "HTTP/1.0",
            Version::Http11 => "HTTP/1.1",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Header {
    pub name: String,
    pub value: String,
}

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

impl RequestHead {
    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }

    pub fn header_all(&self, name: &str) -> Vec<&str> {
        header_all(&self.headers, name)
    }

    pub fn header_contains_token(&self, name: &str, token: &str) -> bool {
        self.header_all(name)
            .iter()
            .any(|v| header_contains_token(v, token))
    }
}

#[derive(Clone, Debug)]
pub struct Request {
    pub head: RequestHead,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.head.header(name)
    }

    pub fn header_all(&self, name: &str) -> Vec<&str> {
        self.head.header_all(name)
    }

    pub fn header_contains_token(&self, name: &str, token: &str) -> bool {
        self.head.header_contains_token(name, token)
    }
}

#[derive(Clone, Debug)]
pub struct ResponseHead {
    pub version: Version,
    pub status: u16,
    pub reason: String,
    pub headers: Vec<Header>,
}

impl ResponseHead {
    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }

    pub fn header_all(&self, name: &str) -> Vec<&str> {
        header_all(&self.headers, name)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyFraming {
    None,
    Length(u64),
    Chunked,
    UntilEof,
}

#[derive(Debug)]
pub struct HttpError {
    pub status: u16,
    pub message: String,
}

impl HttpError {
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(400, message)
    }

    pub fn too_large(message: impl Into<String>) -> Self {
        Self::new(413, message)
    }

    pub fn upstream(message: impl Into<String>) -> Self {
        Self::new(502, message)
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.status, self.message)
    }
}

impl std::error::Error for HttpError {}

/* ------------------------------------------------------------------ */
/* Header helpers                                                     */
/* ------------------------------------------------------------------ */

pub fn header<'a>(headers: &'a [Header], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
}

pub fn header_all<'a>(headers: &'a [Header], name: &str) -> Vec<&'a str> {
    headers
        .iter()
        .filter(|h| h.name.eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
        .collect()
}

pub fn header_contains_token(value: &str, token: &str) -> bool {
    value.split(',').any(|part| {
        part.split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case(token)
    })
}

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

/// Header names listed in the `Connection` header are themselves hop-by-hop.
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

pub fn is_token(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

fn has_ctl(s: &str) -> bool {
    s.chars().any(|c| (c < ' ' && c != '\t') || c == '\x7f')
}

pub fn sanitize_header_value(v: &str) -> String {
    v.chars()
        .filter(|c| *c != '\r' && *c != '\n' && *c != '\0')
        .collect()
}

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

/* ------------------------------------------------------------------ */
/* Low-level line reader                                              */
/* ------------------------------------------------------------------ */

/// Read one line, keeping the original EOL bytes. Returns `None` at EOF when
/// nothing was read. `total` is shared across all lines of one header block.
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

/// Read one raw line from a wire stream (used for chunked encoding and WebSocket
/// handshakes). The returned bytes include the original EOL.
pub async fn read_wire_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Vec<u8>>, HttpError> {
    let mut total = 0usize;
    read_raw_line(reader, &mut total, 502).await
}

fn strip_eol(mut line: Vec<u8>) -> Vec<u8> {
    if line.last() == Some(&b'\n') {
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
    }
    line
}

fn line_string(line: Vec<u8>) -> String {
    String::from_utf8_lossy(&strip_eol(line)).into_owned()
}

/* ------------------------------------------------------------------ */
/* Request parsing                                                    */
/* ------------------------------------------------------------------ */

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
            // Consume trailers until the empty line.
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

/// Reads a complete request (head + body). Returns `None` on a clean EOF.
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

/* ------------------------------------------------------------------ */
/* Response parsing                                                   */
/* ------------------------------------------------------------------ */

/// Read the raw bytes of a response header block (through the final CRLF).
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

pub async fn read_response_head<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<ResponseHead, HttpError> {
    loop {
        let raw = read_raw_response_head(reader).await?;
        let head = parse_response_head(&raw)?;
        if (100..200).contains(&head.status) && head.status != 101 {
            // Informational response (e.g. 100 Continue). We stripped Expect
            // before forwarding, so simply keep reading the final response.
            continue;
        }
        return Ok(head);
    }
}

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

/* ------------------------------------------------------------------ */
/* Response writer                                                    */
/* ------------------------------------------------------------------ */

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

/* ------------------------------------------------------------------ */
/* Tests                                                              */
/* ------------------------------------------------------------------ */

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    async fn head(input: &[u8]) -> RequestHead {
        let mut reader = BufReader::new(input);
        read_request_head(&mut reader).await.unwrap().unwrap()
    }

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

    #[tokio::test]
    async fn http10_keepalive_requires_header() {
        let h = head(b"GET / HTTP/1.0\r\n\r\n").await;
        assert!(!h.keepalive);
        let h = head(b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n").await;
        assert!(h.keepalive);
    }

    #[tokio::test]
    async fn reads_content_length_body() {
        let mut reader = BufReader::new(&b"POST /x HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello"[..]);
        let req = read_request(&mut reader, 1024).await.unwrap().unwrap();
        assert_eq!(req.body, b"hello");
    }

    #[tokio::test]
    async fn decodes_chunked_body() {
        let input =
            b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let mut reader = BufReader::new(&input[..]);
        let req = read_request(&mut reader, 1024).await.unwrap().unwrap();
        assert_eq!(req.body, b"hello world");
    }

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
