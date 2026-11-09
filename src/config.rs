//! nginx-style configuration parser for RPS.
//!
//! Grammar:
//!   directive arg1 arg2 ... ;
//!   directive arg1 ... { ... }
//! `#` starts a comment. Quoted strings are supported. Braces and semicolons
//! must be separated by whitespace only when they are part of a word.

use std::fmt;
use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UseMode {
    /// Event driven. In this rewrite this maps to Tokio's multi-threaded
    /// reactor (epoll on Linux, IOCP on Windows).
    Reactor,
    /// One dedicated OS thread per client connection.
    Thread,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub worker_processes: usize,
    pub daemon: bool,
    pub pid: String,
    pub use_mode: UseMode,
    pub worker_connections: usize,
    pub http: HttpConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            worker_processes: 1,
            daemon: false,
            pid: "run_pid.conf".to_string(),
            use_mode: UseMode::Reactor,
            worker_connections: 512,
            http: HttpConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct HttpConfig {
    pub body_max_size: Option<u64>,
    pub upstreams: Vec<UpstreamConfig>,
    pub servers: Vec<ServerConfig>,
}

#[derive(Clone, Debug)]
pub struct UpstreamConfig {
    pub name: String,
    pub peers: Vec<PeerConfig>,
    pub keepalive: usize,
    pub keepalive_timeout: Duration,
    pub keepalive_requests: usize,
}

#[derive(Clone, Debug)]
pub struct PeerConfig {
    pub host: String,
    pub port: u16,
    pub weight: i64,
    pub max_fails: u32,
    pub fail_timeout: Duration,
    pub down: bool,
    pub backup: bool,
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 80,
            weight: 1,
            max_fails: 1,
            fail_timeout: Duration::from_secs(10),
            down: false,
            backup: false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub listen_ip: Option<IpAddr>,
    pub listen_port: u16,
    pub server_name: Option<String>,
    pub body_max_size: Option<u64>,
    pub locations: Vec<LocationConfig>,
}

#[derive(Clone, Debug)]
pub struct LocationConfig {
    pub pattern: String,
    pub proxy: Option<ProxyConfig>,
    pub body_max_size: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct ProxyConfig {
    pub raw: String,
    /// Candidate `upstream {}` name. At runtime the actual upstream block is
    /// looked up by this name; when absent the host is used directly.
    pub upstream_name: Option<String>,
    pub target_host: String,
    pub target_port: u16,
    /// Explicit path from `proxy_pass` (may include a query string).
    pub target_uri: Option<String>,
    pub set_headers: Vec<(String, String)>,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub send_timeout: Duration,
    pub buffering: bool,
    pub pass_request_headers: bool,
    pub pass_request_body: bool,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            raw: String::new(),
            upstream_name: None,
            target_host: String::new(),
            target_port: 80,
            target_uri: None,
            set_headers: Vec::new(),
            connect_timeout: Duration::from_millis(60_000),
            read_timeout: Duration::from_millis(60_000),
            send_timeout: Duration::from_millis(60_000),
            buffering: true,
            pass_request_headers: true,
            pass_request_body: true,
        }
    }
}

#[derive(Debug)]
pub struct ConfigError {
    pub line: usize,
    pub message: String,
}

impl ConfigError {
    fn new(line: usize, message: impl Into<String>) -> Self {
        Self {
            line,
            message: message.into(),
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.line > 0 {
            write!(f, "line {}: {}", self.line, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl std::error::Error for ConfigError {}

/* ------------------------------------------------------------------ */
/* Lexer                                                              */
/* ------------------------------------------------------------------ */

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Word(String),
    LBrace,
    RBrace,
    Semi,
    Eof,
}

struct Lexer {
    chars: Vec<char>,
    pos: usize,
    line: usize,
}

impl Lexer {
    fn new(text: &str) -> Self {
        Self {
            chars: text.chars().collect(),
            pos: 0,
            line: 1,
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += 1;
        if c == '\n' {
            self.line += 1;
        }
        Some(c)
    }

    fn skip_trivia(&mut self) {
        loop {
            match self.peek() {
                Some(c) if c.is_whitespace() => {
                    self.bump();
                }
                Some('#') => {
                    while let Some(c) = self.peek() {
                        if c == '\n' {
                            break;
                        }
                        self.bump();
                    }
                }
                _ => break,
            }
        }
    }

    fn next_token(&mut self) -> Result<(Token, usize), ConfigError> {
        self.skip_trivia();
        let line = self.line;
        match self.peek() {
            None => Ok((Token::Eof, line)),
            Some('{') => {
                self.bump();
                Ok((Token::LBrace, line))
            }
            Some('}') => {
                self.bump();
                Ok((Token::RBrace, line))
            }
            Some(';') => {
                self.bump();
                Ok((Token::Semi, line))
            }
            Some('"') => {
                self.bump();
                let mut value = String::new();
                loop {
                    match self.bump() {
                        None => return Err(ConfigError::new(line, "unterminated quoted string")),
                        Some('"') => break,
                        Some('\\') => match self.bump() {
                            Some('n') => value.push('\n'),
                            Some('t') => value.push('\t'),
                            Some('r') => value.push('\r'),
                            Some(c) => value.push(c),
                            None => {
                                return Err(ConfigError::new(
                                    line,
                                    "unterminated escape sequence in quoted string",
                                ))
                            }
                        },
                        Some(c) => value.push(c),
                    }
                }
                Ok((Token::Word(value), line))
            }
            Some(_) => {
                let mut value = String::new();
                while let Some(c) = self.peek() {
                    if c.is_whitespace() || matches!(c, '{' | '}' | ';' | '#') {
                        break;
                    }
                    value.push(c);
                    self.bump();
                }
                if value.is_empty() {
                    return Err(ConfigError::new(
                        line,
                        format!("unexpected character '{}'", self.peek().unwrap_or('?')),
                    ));
                }
                Ok((Token::Word(value), line))
            }
        }
    }
}

/* ------------------------------------------------------------------ */
/* Syntax tree                                                        */
/* ------------------------------------------------------------------ */

#[derive(Debug)]
struct Directive {
    name: String,
    args: Vec<String>,
    children: Vec<Directive>,
    line: usize,
}

fn parse_directives(lexer: &mut Lexer, nested: bool) -> Result<Vec<Directive>, ConfigError> {
    let mut out = Vec::new();
    loop {
        let (token, line) = lexer.next_token()?;
        match token {
            Token::Eof => {
                if nested {
                    return Err(ConfigError::new(
                        line,
                        "unexpected end of file, missing '}'",
                    ));
                }
                return Ok(out);
            }
            Token::RBrace => {
                if nested {
                    return Ok(out);
                }
                return Err(ConfigError::new(line, "unexpected '}'"));
            }
            Token::Semi => {
                // Tolerate stray semicolons.
                continue;
            }
            Token::LBrace => {
                return Err(ConfigError::new(line, "unexpected '{'"));
            }
            Token::Word(name) => {
                let mut args = Vec::new();
                let children;
                loop {
                    let (next, next_line) = lexer.next_token()?;
                    match next {
                        Token::Word(word) => args.push(word),
                        Token::Semi => {
                            children = Vec::new();
                            break;
                        }
                        Token::LBrace => {
                            children = parse_directives(lexer, true)?;
                            break;
                        }
                        Token::RBrace => {
                            return Err(ConfigError::new(
                                next_line,
                                format!("directive \"{}\" must end with ';' or '{{...}}'", name),
                            ))
                        }
                        Token::Eof => {
                            return Err(ConfigError::new(
                                next_line,
                                format!("unexpected end of file after directive \"{}\"", name),
                            ))
                        }
                    }
                }
                out.push(Directive {
                    name,
                    args,
                    children,
                    line,
                });
            }
        }
    }
}

/* ------------------------------------------------------------------ */
/* Entry point                                                        */
/* ------------------------------------------------------------------ */

pub fn parse_config(path: &Path) -> Result<Config, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        ConfigError::new(0, format!("failed to read \"{}\": {}", path.display(), e))
    })?;
    let text = text.trim_start_matches('\u{feff}');
    let mut lexer = Lexer::new(text);
    let directives = parse_directives(&mut lexer, false)?;
    parse_main(&directives)
}

fn parse_main(directives: &[Directive]) -> Result<Config, ConfigError> {
    let mut cfg = Config::default();
    let mut worker_processes: Option<usize> = None;
    let mut daemon: Option<bool> = None;
    let mut pid: Option<String> = None;
    let mut event_seen = false;
    let mut http_seen = false;

    for d in directives {
        match d.name.as_str() {
            "worker_processes" => {
                if worker_processes.is_some() {
                    return Err(ConfigError::new(
                        d.line,
                        "\"worker_processes\" is duplicate",
                    ));
                }
                let v = one_arg(d)?;
                let n = if v.eq_ignore_ascii_case("auto") {
                    1
                } else {
                    parse_usize(v, d.line, "worker_processes")?
                };
                if n == 0 {
                    return Err(ConfigError::new(d.line, "worker_processes must be >= 1"));
                }
                worker_processes = Some(n);
            }
            "daemon" => {
                if daemon.is_some() {
                    return Err(ConfigError::new(d.line, "\"daemon\" is duplicate"));
                }
                daemon = Some(parse_on_off(one_arg(d)?, d.line, "daemon")?);
            }
            "pid" => {
                if pid.is_some() {
                    return Err(ConfigError::new(d.line, "\"pid\" is duplicate"));
                }
                pid = Some(one_arg(d)?.to_string());
            }
            "event" => {
                if event_seen {
                    return Err(ConfigError::new(d.line, "\"event\" block is duplicate"));
                }
                event_seen = true;
                parse_event(d, &mut cfg)?;
            }
            "http" => {
                if http_seen {
                    return Err(ConfigError::new(d.line, "\"http\" block is duplicate"));
                }
                http_seen = true;
                cfg.http = parse_http(d)?;
            }
            other => {
                return Err(ConfigError::new(
                    d.line,
                    format!("unknown directive \"{}\" in main context", other),
                ))
            }
        }
    }

    if let Some(v) = worker_processes {
        cfg.worker_processes = v;
    }
    if let Some(v) = daemon {
        cfg.daemon = v;
    }
    if let Some(v) = pid {
        cfg.pid = v;
    }
    Ok(cfg)
}

fn parse_event(d: &Directive, cfg: &mut Config) -> Result<(), ConfigError> {
    if !d.args.is_empty() {
        return Err(ConfigError::new(d.line, "event must not have arguments"));
    }
    if d.children.is_empty() {
        return Err(ConfigError::new(d.line, "event block is empty"));
    }
    let mut use_seen = false;
    let mut conns_seen = false;

    for c in &d.children {
        match c.name.as_str() {
            "use" => {
                if use_seen {
                    return Err(ConfigError::new(c.line, "\"use\" is duplicate"));
                }
                use_seen = true;
                let v = one_arg(c)?;
                cfg.use_mode = match v {
                    "epoll" | "io_uring" => UseMode::Reactor,
                    "multithreading" => UseMode::Thread,
                    other => {
                        return Err(ConfigError::new(
                            c.line,
                            format!(
                                "use must be epoll, io_uring or multithreading, got \"{}\"",
                                other
                            ),
                        ))
                    }
                };
            }
            "worker_connections" => {
                if conns_seen {
                    return Err(ConfigError::new(
                        c.line,
                        "\"worker_connections\" is duplicate",
                    ));
                }
                conns_seen = true;
                let n = parse_usize(one_arg(c)?, c.line, "worker_connections")?;
                if n == 0 {
                    return Err(ConfigError::new(c.line, "worker_connections must be >= 1"));
                }
                cfg.worker_connections = n;
            }
            other => {
                return Err(ConfigError::new(
                    c.line,
                    format!("unknown directive \"{}\" in event context", other),
                ))
            }
        }
    }
    Ok(())
}

fn parse_http(d: &Directive) -> Result<HttpConfig, ConfigError> {
    if !d.args.is_empty() {
        return Err(ConfigError::new(d.line, "http must not have arguments"));
    }
    let mut http = HttpConfig::default();
    let mut body_max_seen = false;
    let mut upstream_names: Vec<String> = Vec::new();

    for c in &d.children {
        match c.name.as_str() {
            "upstream" => {
                let u = parse_upstream(c)?;
                if upstream_names.iter().any(|n| n == &u.name) {
                    return Err(ConfigError::new(
                        c.line,
                        format!("duplicate upstream \"{}\"", u.name),
                    ));
                }
                upstream_names.push(u.name.clone());
                http.upstreams.push(u);
            }
            "server" => {
                http.servers.push(parse_server(c)?);
            }
            "http_body_max_size" => {
                if body_max_seen {
                    return Err(ConfigError::new(
                        c.line,
                        "\"http_body_max_size\" is duplicate",
                    ));
                }
                body_max_seen = true;
                http.body_max_size = Some(parse_size(one_arg(c)?, c.line, "http_body_max_size")?);
            }
            other => {
                return Err(ConfigError::new(
                    c.line,
                    format!("unknown directive \"{}\" in http context", other),
                ))
            }
        }
    }
    Ok(http)
}

fn parse_upstream(d: &Directive) -> Result<UpstreamConfig, ConfigError> {
    if d.args.len() != 1 {
        return Err(ConfigError::new(
            d.line,
            "upstream requires exactly one name",
        ));
    }
    if !d.children.is_empty() {
        // ok
    } else {
        return Err(ConfigError::new(d.line, "upstream block is empty"));
    }

    let mut out = UpstreamConfig {
        name: d.args[0].clone(),
        peers: Vec::new(),
        keepalive: 0,
        keepalive_timeout: Duration::from_millis(60_000),
        keepalive_requests: 100,
    };
    let mut keepalive_seen = false;
    let mut timeout_seen = false;
    let mut requests_seen = false;

    for c in &d.children {
        match c.name.as_str() {
            "server" => out.peers.push(parse_peer(c)?),
            "keepalive" => {
                if keepalive_seen {
                    return Err(ConfigError::new(c.line, "\"keepalive\" is duplicate"));
                }
                keepalive_seen = true;
                out.keepalive = parse_usize(one_arg(c)?, c.line, "keepalive")?;
            }
            "keepalive_timeout" => {
                if timeout_seen {
                    return Err(ConfigError::new(
                        c.line,
                        "\"keepalive_timeout\" is duplicate",
                    ));
                }
                timeout_seen = true;
                out.keepalive_timeout = parse_msec(one_arg(c)?, c.line, "keepalive_timeout")?;
            }
            "keepalive_requests" => {
                if requests_seen {
                    return Err(ConfigError::new(
                        c.line,
                        "\"keepalive_requests\" is duplicate",
                    ));
                }
                requests_seen = true;
                out.keepalive_requests = parse_usize(one_arg(c)?, c.line, "keepalive_requests")?;
            }
            other => {
                return Err(ConfigError::new(
                    c.line,
                    format!("unknown directive \"{}\" in upstream context", other),
                ))
            }
        }
    }

    if out.peers.is_empty() {
        return Err(ConfigError::new(
            d.line,
            format!("upstream \"{}\" has no server", out.name),
        ));
    }
    Ok(out)
}

fn parse_peer(d: &Directive) -> Result<PeerConfig, ConfigError> {
    if d.args.is_empty() {
        return Err(ConfigError::new(
            d.line,
            "upstream server requires an address",
        ));
    }
    let (host, port) = split_host_port(&d.args[0], 80, d.line)?;
    if host.is_empty() {
        return Err(ConfigError::new(d.line, "upstream server host is empty"));
    }

    let mut peer = PeerConfig {
        host,
        port,
        ..PeerConfig::default()
    };

    for arg in &d.args[1..] {
        if let Some(v) = arg.strip_prefix("weight=") {
            peer.weight = parse_i64(v, d.line, "weight")?;
            if peer.weight < 0 {
                return Err(ConfigError::new(d.line, "weight must be >= 0"));
            }
        } else if let Some(v) = arg.strip_prefix("max_fails=") {
            peer.max_fails = parse_u32(v, d.line, "max_fails")?;
        } else if let Some(v) = arg.strip_prefix("fail_timeout=") {
            peer.fail_timeout = parse_fail_timeout(v, d.line)?;
        } else if arg.eq_ignore_ascii_case("down") {
            peer.down = true;
        } else if arg.eq_ignore_ascii_case("backup") {
            peer.backup = true;
        } else {
            return Err(ConfigError::new(
                d.line,
                format!("unknown upstream server parameter \"{}\"", arg),
            ));
        }
    }
    Ok(peer)
}

fn parse_server(d: &Directive) -> Result<ServerConfig, ConfigError> {
    if !d.args.is_empty() {
        return Err(ConfigError::new(d.line, "server must not have arguments"));
    }
    let mut listen: Option<(Option<IpAddr>, u16)> = None;
    let mut server_name: Option<String> = None;
    let mut body_max_size: Option<u64> = None;
    let mut locations = Vec::new();

    for c in &d.children {
        match c.name.as_str() {
            "listen" => {
                if listen.is_some() {
                    return Err(ConfigError::new(c.line, "\"listen\" is duplicate"));
                }
                listen = Some(parse_listen(one_arg(c)?, c.line)?);
            }
            "server_name" => {
                if server_name.is_some() {
                    return Err(ConfigError::new(c.line, "\"server_name\" is duplicate"));
                }
                server_name = Some(one_arg(c)?.to_string());
            }
            "location" => locations.push(parse_location(c)?),
            "http_body_max_size" => {
                if body_max_size.is_some() {
                    return Err(ConfigError::new(
                        c.line,
                        "\"http_body_max_size\" is duplicate",
                    ));
                }
                body_max_size = Some(parse_size(one_arg(c)?, c.line, "http_body_max_size")?);
            }
            other => {
                return Err(ConfigError::new(
                    c.line,
                    format!("unknown directive \"{}\" in server context", other),
                ))
            }
        }
    }

    let (listen_ip, listen_port) =
        listen.ok_or_else(|| ConfigError::new(d.line, "server block requires \"listen\""))?;

    Ok(ServerConfig {
        listen_ip,
        listen_port,
        server_name,
        body_max_size,
        locations,
    })
}

fn parse_location(d: &Directive) -> Result<LocationConfig, ConfigError> {
    if d.args.len() != 1 {
        return Err(ConfigError::new(
            d.line,
            "location requires exactly one path pattern",
        ));
    }
    if d.children.is_empty() {
        return Err(ConfigError::new(d.line, "location block is empty"));
    }
    let pattern = d.args[0].clone();
    if pattern.is_empty() {
        return Err(ConfigError::new(
            d.line,
            "location pattern must not be empty",
        ));
    }

    let mut proxy: Option<ProxyConfig> = None;
    let mut proxy_pass_seen = false;
    let mut body_max_size: Option<u64> = None;

    for c in &d.children {
        match c.name.as_str() {
            "proxy_pass" => {
                if proxy_pass_seen {
                    return Err(ConfigError::new(c.line, "\"proxy_pass\" is duplicate"));
                }
                proxy_pass_seen = true;
                let raw = one_arg(c)?;
                let parsed = parse_proxy_url(raw, c.line)?;
                let p = proxy.get_or_insert_with(ProxyConfig::default);
                p.raw = raw.to_string();
                p.upstream_name = parsed.upstream_name;
                p.target_host = parsed.host;
                p.target_port = parsed.port;
                p.target_uri = parsed.path;
            }
            "proxy_set_header" => {
                if c.args.len() != 2 {
                    return Err(ConfigError::new(
                        c.line,
                        "proxy_set_header requires exactly 2 arguments",
                    ));
                }
                let p = proxy.get_or_insert_with(|| ProxyConfig {
                    raw: String::new(),
                    ..ProxyConfig::default()
                });
                validate_header_name(&c.args[0], c.line)?;
                p.set_headers.push((c.args[0].clone(), c.args[1].clone()));
            }
            "proxy_connect_timeout" => {
                let v = parse_msec(one_arg(c)?, c.line, "proxy_connect_timeout")?;
                proxy
                    .get_or_insert_with(ProxyConfig::default)
                    .connect_timeout = v;
            }
            "proxy_read_timeout" => {
                let v = parse_msec(one_arg(c)?, c.line, "proxy_read_timeout")?;
                proxy.get_or_insert_with(ProxyConfig::default).read_timeout = v;
            }
            "proxy_send_timeout" => {
                let v = parse_msec(one_arg(c)?, c.line, "proxy_send_timeout")?;
                proxy.get_or_insert_with(ProxyConfig::default).send_timeout = v;
            }
            "proxy_buffering" => {
                let v = parse_on_off(one_arg(c)?, c.line, "proxy_buffering")?;
                proxy.get_or_insert_with(ProxyConfig::default).buffering = v;
            }
            "proxy_pass_request_headers" => {
                let v = parse_on_off(one_arg(c)?, c.line, "proxy_pass_request_headers")?;
                proxy
                    .get_or_insert_with(ProxyConfig::default)
                    .pass_request_headers = v;
            }
            "proxy_pass_request_body" => {
                let v = parse_on_off(one_arg(c)?, c.line, "proxy_pass_request_body")?;
                proxy
                    .get_or_insert_with(ProxyConfig::default)
                    .pass_request_body = v;
            }
            "http_body_max_size" => {
                if body_max_size.is_some() {
                    return Err(ConfigError::new(
                        c.line,
                        "\"http_body_max_size\" is duplicate",
                    ));
                }
                body_max_size = Some(parse_size(one_arg(c)?, c.line, "http_body_max_size")?);
            }
            other => {
                return Err(ConfigError::new(
                    c.line,
                    format!("unknown directive \"{}\" in location context", other),
                ))
            }
        }
    }

    // A location that only sets proxy_* options without proxy_pass is valid:
    // it falls through to the built-in default response, like the original.
    if !proxy_pass_seen {
        proxy = None;
    }

    Ok(LocationConfig {
        pattern,
        proxy,
        body_max_size,
    })
}

/* ------------------------------------------------------------------ */
/* Value parsers                                                      */
/* ------------------------------------------------------------------ */

fn one_arg(d: &Directive) -> Result<&str, ConfigError> {
    if d.args.len() != 1 {
        return Err(ConfigError::new(
            d.line,
            format!("directive \"{}\" requires exactly one argument", d.name),
        ));
    }
    Ok(&d.args[0])
}

fn parse_usize(v: &str, line: usize, what: &str) -> Result<usize, ConfigError> {
    v.parse::<usize>()
        .map_err(|_| ConfigError::new(line, format!("{} must be a non-negative integer", what)))
}

fn parse_u32(v: &str, line: usize, what: &str) -> Result<u32, ConfigError> {
    v.parse::<u32>()
        .map_err(|_| ConfigError::new(line, format!("{} must be a non-negative integer", what)))
}

fn parse_i64(v: &str, line: usize, what: &str) -> Result<i64, ConfigError> {
    v.parse::<i64>()
        .map_err(|_| ConfigError::new(line, format!("{} must be an integer", what)))
}

fn parse_on_off(v: &str, line: usize, what: &str) -> Result<bool, ConfigError> {
    if v.eq_ignore_ascii_case("on") {
        Ok(true)
    } else if v.eq_ignore_ascii_case("off") {
        Ok(false)
    } else {
        Err(ConfigError::new(
            line,
            format!("{} must be \"on\" or \"off\"", what),
        ))
    }
}

/// Parse a byte size. Bare integers are bytes; suffixes k/K/m/M/g/G are
/// accepted, optionally followed by "b".
fn parse_size(v: &str, line: usize, what: &str) -> Result<u64, ConfigError> {
    let s = v.trim();
    let lower = s.to_ascii_lowercase();
    let (num, mult) = if let Some(n) = lower.strip_suffix("kb") {
        (n, 1024u64)
    } else if let Some(n) = lower.strip_suffix("mb") {
        (n, 1024u64 * 1024)
    } else if let Some(n) = lower.strip_suffix("gb") {
        (n, 1024u64 * 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix('k') {
        (n, 1024)
    } else if let Some(n) = lower.strip_suffix('m') {
        (n, 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix('g') {
        (n, 1024 * 1024 * 1024)
    } else {
        (lower.as_str(), 1)
    };
    let base = num
        .parse::<u64>()
        .map_err(|_| ConfigError::new(line, format!("{} must be a number", what)))?;
    base.checked_mul(mult)
        .ok_or_else(|| ConfigError::new(line, format!("{} is too large", what)))
}

/// Milliseconds. Bare numbers are milliseconds (matching the original
/// proxy_*_timeout directives); "s"/"ms" suffixes are also accepted.
fn parse_msec(v: &str, line: usize, what: &str) -> Result<Duration, ConfigError> {
    let s = v.trim().to_ascii_lowercase();
    let ms = if let Some(n) = s.strip_suffix("ms") {
        n.parse::<u64>()
            .map_err(|_| ConfigError::new(line, format!("{} must be a number", what)))?
    } else if let Some(n) = s.strip_suffix('s') {
        n.parse::<u64>()
            .map_err(|_| ConfigError::new(line, format!("{} must be a number", what)))?
            .saturating_mul(1000)
    } else {
        s.parse::<u64>()
            .map_err(|_| ConfigError::new(line, format!("{} must be a number", what)))?
    };
    Ok(Duration::from_millis(ms))
}

/// fail_timeout follows the original semantics: bare number = seconds.
fn parse_fail_timeout(v: &str, line: usize) -> Result<Duration, ConfigError> {
    let s = v.trim().to_ascii_lowercase();
    if let Some(n) = s.strip_suffix("ms") {
        let ms = n
            .parse::<u64>()
            .map_err(|_| ConfigError::new(line, "fail_timeout must be a number"))?;
        Ok(Duration::from_millis(ms))
    } else if let Some(n) = s.strip_suffix('s') {
        let secs = n
            .parse::<u64>()
            .map_err(|_| ConfigError::new(line, "fail_timeout must be a number"))?;
        Ok(Duration::from_secs(secs))
    } else {
        let secs = s
            .parse::<u64>()
            .map_err(|_| ConfigError::new(line, "fail_timeout must be a number"))?;
        Ok(Duration::from_secs(secs))
    }
}

fn parse_listen(v: &str, line: usize) -> Result<(Option<IpAddr>, u16), ConfigError> {
    let s = v.trim();
    if let Ok(port) = s.parse::<u16>() {
        if port == 0 {
            return Err(ConfigError::new(line, "listen port must be in 1..=65535"));
        }
        return Ok((None, port));
    }

    let (host, port_str) = if let Some(rest) = s.strip_prefix('[') {
        let (h, tail) = rest
            .split_once(']')
            .ok_or_else(|| ConfigError::new(line, "invalid bracketed IPv6 listen address"))?;
        let p = tail
            .strip_prefix(':')
            .ok_or_else(|| ConfigError::new(line, "listen IPv6 address is missing a port"))?;
        (h, p)
    } else {
        s.rsplit_once(':')
            .ok_or_else(|| ConfigError::new(line, "listen requires <port> or <addr>:<port>"))?
    };

    let port = port_str
        .parse::<u16>()
        .map_err(|_| ConfigError::new(line, format!("invalid listen port \"{}\"", port_str)))?;
    if port == 0 {
        return Err(ConfigError::new(line, "listen port must be in 1..=65535"));
    }

    if host.is_empty() || host == "*" || host == "0.0.0.0" {
        return Ok((None, port));
    }
    let ip = host.parse::<IpAddr>().map_err(|_| {
        ConfigError::new(
            line,
            format!("listen address \"{}\" must be an IP address or '*'", host),
        )
    })?;
    Ok((Some(ip), port))
}

fn split_host_port(v: &str, default_port: u16, line: usize) -> Result<(String, u16), ConfigError> {
    let s = v.trim();
    if s.is_empty() {
        return Err(ConfigError::new(line, "address must not be empty"));
    }
    if let Some(rest) = s.strip_prefix('[') {
        let (host, tail) = rest
            .split_once(']')
            .ok_or_else(|| ConfigError::new(line, "invalid bracketed IPv6 address"))?;
        let port = if let Some(p) = tail.strip_prefix(':') {
            p.parse::<u16>()
                .map_err(|_| ConfigError::new(line, format!("invalid port \"{}\"", p)))?
        } else if tail.is_empty() {
            default_port
        } else {
            return Err(ConfigError::new(line, "invalid IPv6 address suffix"));
        };
        return Ok((host.to_string(), port));
    }
    if let Some((host, p)) = s.rsplit_once(':') {
        if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) {
            let port = p
                .parse::<u16>()
                .map_err(|_| ConfigError::new(line, format!("invalid port \"{}\"", p)))?;
            if port == 0 {
                return Err(ConfigError::new(line, "port must be in 1..=65535"));
            }
            return Ok((host.to_string(), port));
        }
    }
    Ok((s.to_string(), default_port))
}

fn validate_header_name(name: &str, line: usize) -> Result<(), ConfigError> {
    if name.is_empty() {
        return Err(ConfigError::new(line, "header name must not be empty"));
    }
    for c in name.chars() {
        if !(c.is_ascii_alphanumeric()
            || matches!(
                c,
                '!' | '#'
                    | '$'
                    | '%'
                    | '&'
                    | '\''
                    | '*'
                    | '+'
                    | '-'
                    | '.'
                    | '^'
                    | '_'
                    | '`'
                    | '|'
                    | '~'
            ))
        {
            return Err(ConfigError::new(
                line,
                format!("invalid character in header name \"{}\"", name),
            ));
        }
    }
    Ok(())
}

struct ParsedProxyUrl {
    host: String,
    port: u16,
    path: Option<String>,
    upstream_name: Option<String>,
}

fn parse_proxy_url(raw: &str, line: usize) -> Result<ParsedProxyUrl, ConfigError> {
    let s = raw.trim();
    if s.is_empty() {
        return Err(ConfigError::new(line, "proxy_pass requires a URL"));
    }

    let rest = if let Some(idx) = s.find("://") {
        let scheme = s[..idx].to_ascii_lowercase();
        if scheme == "https" {
            return Err(ConfigError::new(
                line,
                "proxy_pass: TLS/HTTPS is not supported (the original RPS has no TLS either)",
            ));
        }
        if scheme != "http" {
            return Err(ConfigError::new(
                line,
                format!("proxy_pass: unsupported scheme \"{}\"", scheme),
            ));
        }
        &s[idx + 3..]
    } else {
        s
    };

    if rest.is_empty() {
        return Err(ConfigError::new(line, "proxy_pass URL has no host"));
    }
    if rest.contains('@') {
        return Err(ConfigError::new(
            line,
            "proxy_pass: credentials in URLs are not supported",
        ));
    }

    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], Some(rest[i..].to_string())),
        None => (rest, None),
    };
    if authority.is_empty() {
        return Err(ConfigError::new(line, "proxy_pass URL has no host"));
    }

    let (host, port) = split_host_port(authority, 80, line)?;
    if host.is_empty() {
        return Err(ConfigError::new(line, "proxy_pass URL has an empty host"));
    }

    // The original treats a host without a dot on port 80 as an upstream {}
    // reference candidate. We do the same, then fall back to a direct host.
    let upstream_name = if port == 80 && !host.contains('.') && !host.contains(':') {
        Some(host.clone())
    } else {
        None
    };

    Ok(ParsedProxyUrl {
        host,
        port,
        path,
        upstream_name,
    })
}

/* ------------------------------------------------------------------ */
/* Tests                                                              */
/* ------------------------------------------------------------------ */

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<Config, ConfigError> {
        let mut lexer = Lexer::new(text);
        let dirs = parse_directives(&mut lexer, false)?;
        parse_main(&dirs)
    }

    #[test]
    fn parses_basic_config() {
        let cfg = parse(
            r#"
            worker_processes 4;
            daemon on;
            pid run.pid;
            event { use epoll; worker_connections 1024; }
            http {
                upstream backend {
                    server 127.0.0.1:8080 weight=3 max_fails=2 fail_timeout=10s;
                    server 127.0.0.1:8081 backup;
                    keepalive 32;
                    keepalive_timeout 60s;
                    keepalive_requests 100;
                }
                server {
                    listen 8000;
                    server_name "example.com";
                    location / {
                        proxy_pass http://backend;
                        proxy_set_header Host $host;
                        proxy_connect_timeout 30000;
                    }
                }
            }
            "#,
        )
        .unwrap();
        assert_eq!(cfg.worker_processes, 4);
        assert!(cfg.daemon);
        assert_eq!(cfg.use_mode, UseMode::Reactor);
        assert_eq!(cfg.worker_connections, 1024);
        assert_eq!(cfg.http.upstreams.len(), 1);
        assert_eq!(cfg.http.upstreams[0].keepalive, 32);
        assert_eq!(cfg.http.upstreams[0].peers.len(), 2);
        assert_eq!(cfg.http.servers[0].listen_port, 8000);
        let loc = &cfg.http.servers[0].locations[0];
        assert_eq!(
            loc.proxy.as_ref().unwrap().upstream_name.as_deref(),
            Some("backend")
        );
    }

    #[test]
    fn quoted_and_unquoted_values() {
        let cfg = parse(
            r#"
            http {
                server {
                    listen 127.0.0.1:8080;
                    location "/api" {
                        proxy_pass "http://127.0.0.1:9090/headers";
                        proxy_set_header X-Test "hello world";
                    }
                }
            }
            "#,
        )
        .unwrap();
        assert_eq!(cfg.http.servers[0].listen_port, 8080);
        let p = cfg.http.servers[0].locations[0].proxy.as_ref().unwrap();
        assert_eq!(p.target_host, "127.0.0.1");
        assert_eq!(p.target_port, 9090);
        assert_eq!(p.target_uri.as_deref(), Some("/headers"));
        assert_eq!(p.set_headers[0].1, "hello world");
    }

    #[test]
    fn rejects_duplicate_directives() {
        let err = parse("worker_processes 1; worker_processes 2;").unwrap_err();
        assert!(err.message.contains("duplicate"));
    }
}
