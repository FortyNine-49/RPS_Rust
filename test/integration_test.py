#!/usr/bin/env python3
"""End-to-end tests for the Rust RPS rewrite.

Starts two HTTP backends and one WebSocket backend, then exercises the proxy
in reactor and thread-per-connection modes, including daemon start/stop.
"""

import base64
import hashlib
import http.client
import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IS_WINDOWS = os.name == "nt"
_DEFAULT_BIN = ROOT / "target" / "debug" / ("rps.exe" if IS_WINDOWS else "rps")
BIN = Path(os.environ["RPS_BIN"]) if os.environ.get("RPS_BIN") else _DEFAULT_BIN
PY = sys.executable
WS_MAGIC = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

passed = 0
failed = 0
procs = []
logs = []


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_port(port, timeout=10.0, open_=True):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            sock = socket.create_connection(("127.0.0.1", port), 0.25)
            sock.close()
            if open_:
                return True
        except OSError:
            if not open_:
                return True
        time.sleep(0.05)
    return False


def start(args, log_path, **kwargs):
    log = open(log_path, "ab")
    logs.append(log)
    proc = subprocess.Popen(args, stdout=log, stderr=log, **kwargs)
    procs.append(proc)
    return proc


def stop(proc):
    if proc is None:
        return
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                pass
    if proc in procs:
        procs.remove(proc)


def check(name, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  PASS: {name}")
        passed += 1
    else:
        print(f"  FAIL: {name}")
        if detail:
            print(f"        {detail}")
        failed += 1


def http_request(port, method, path, body=None, headers=None, timeout=8.0):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        data = resp.read()
        hdrs = {k.lower(): v for k, v in resp.getheaders()}
        return resp.status, data, hdrs
    finally:
        conn.close()


def recv_until(sock, marker, limit=65536):
    data = b""
    while marker not in data and len(data) < limit:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    return data


def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise RuntimeError("socket closed")
        data += chunk
    return data


def make_ws_frame(opcode, payload, mask=True):
    header = bytes([0x80 | opcode])
    length = len(payload)
    mask_bit = 0x80 if mask else 0
    if length < 126:
        header += bytes([mask_bit | length])
    elif length < 65536:
        header += bytes([mask_bit | 126]) + struct.pack(">H", length)
    else:
        header += bytes([mask_bit | 127]) + struct.pack(">Q", length)
    if not mask:
        return header + payload
    key = b"\x12\x34\x56\x78"
    masked = bytes(b ^ key[i % 4] for i, b in enumerate(payload))
    return header + key + masked


def read_ws_frame(sock):
    head = recv_exact(sock, 2)
    b1, b2 = head[0], head[1]
    opcode = b1 & 0x0F
    masked = (b2 & 0x80) != 0
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack(">H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack(">Q", recv_exact(sock, 8))[0]
    key = recv_exact(sock, 4) if masked else b""
    payload = recv_exact(sock, length)
    if masked:
        payload = bytes(b ^ key[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def websocket_test(proxy_port, ws_path="/ws"):
    key = base64.b64encode(os.urandom(16)).decode()
    sock = socket.create_connection(("127.0.0.1", proxy_port), timeout=8)
    request = (
        f"GET {ws_path} HTTP/1.1\r\n"
        "Host: test.local\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    )
    sock.sendall(request.encode())
    response = recv_until(sock, b"\r\n\r\n")
    text = response.decode("latin1")
    expected_accept = base64.b64encode(
        hashlib.sha1(key.encode() + WS_MAGIC).digest()
    ).decode()
    ok_handshake = (
        text.startswith("HTTP/1.1 101")
        and "upgrade: websocket" in text.lower()
        and expected_accept in text
    )
    if not ok_handshake:
        sock.close()
        return False, f"handshake response: {text[:300]}"

    sock.sendall(make_ws_frame(0x1, b"hello ws"))
    opcode, payload = read_ws_frame(sock)
    ok_echo = opcode == 0x1 and payload == b"hello ws"

    sock.sendall(make_ws_frame(0x9, b"ping"))
    pong_opcode, pong_payload = read_ws_frame(sock)
    ok_ping = pong_opcode == 0xA and pong_payload == b"ping"

    sock.sendall(make_ws_frame(0x8, b""))
    sock.close()
    return ok_echo and ok_ping, f"echo={ok_echo} ping={ok_ping}"


def config_text(proxy_port, http_port, http_port2, ws_port, mode, daemon=False, pid=None):
    daemon_line = "daemon on;" if daemon else "daemon off;"
    pid_line = ""
    if pid:
        pid_line = 'pid "{}";'.format(str(pid).replace("\\", "/"))
    return f"""
worker_processes 2;
{daemon_line}
{pid_line}
event {{
    use {mode};
    worker_connections 128;
}}

http {{
    upstream backend {{
        server 127.0.0.1:{http_port} weight=2 max_fails=1 fail_timeout=2s;
        server 127.0.0.1:{http_port2} weight=1;
        keepalive 8;
        keepalive_timeout 10s;
        keepalive_requests 100;
    }}

    upstream single {{
        server 127.0.0.1:{http_port};
        keepalive 8;
    }}

    server {{
        listen 127.0.0.1:{proxy_port};
        server_name "test.local";

        location / {{
            proxy_pass http://backend;
        }}
        location /single {{
            proxy_pass http://single/;
        }}
        location /hdr {{
            proxy_pass http://backend/headers;
            proxy_set_header X-Test yes;
        }}
        location /api {{
            proxy_pass http://127.0.0.1:{http_port}/headers;
        }}
        location /chunk {{
            proxy_pass http://127.0.0.1:{http_port}/chunked;
        }}
        location /post {{
            proxy_pass http://127.0.0.1:{http_port}/echo;
        }}
        location /no-headers {{
            proxy_pass http://127.0.0.1:{http_port}/headers;
            proxy_pass_request_headers off;
        }}
        location /dead {{
            proxy_pass http://127.0.0.1:1;
            proxy_connect_timeout 1000;
        }}
        location /default {{
            proxy_set_header X-Ignored yes;
        }}
        location /ws {{
            proxy_pass http://127.0.0.1:{ws_port};
        }}
    }}
}}
"""


def run_suite(proxy_port, label):
    print(f"\n=== {label}: proxy on 127.0.0.1:{proxy_port} ===")

    status, body, hdrs = http_request(proxy_port, "GET", "/")
    check("basic GET", status == 200 and b"backend " in body, f"{status} {body!r}")

    status, body, hdrs = http_request(proxy_port, "GET", "/headers")
    check("response status/body forwarding", status == 200 and b"request:" in body)
    check("Server header", "server" in hdrs and hdrs["server"] != "")

    # Load balancing: 30 requests, smooth WRR at 2:1 => 20/10.
    counts = {}
    for _ in range(30):
        status, body, _ = http_request(proxy_port, "GET", "/", headers={"Connection": "close"})
        text = body.decode("latin1", "replace")
        parts = text.split()
        if status == 200 and len(parts) >= 2:
            counts[parts[1]] = counts.get(parts[1], 0) + 1
    values = sorted(counts.values(), reverse=True)
    check(
        "weighted round-robin 2:1",
        len(values) == 2 and values[0] == 20 and values[1] == 10,
        f"counts={counts}",
    )

    # Upstream keepalive: the same backend connection should be reused.
    conn = http.client.HTTPConnection("127.0.0.1", proxy_port, timeout=8)
    conn_ids = []
    ok = True
    for _ in range(3):
        try:
            conn.request("GET", "/single")
            resp = conn.getresponse()
            data = resp.read().decode("latin1", "replace")
            ok = ok and resp.status == 200
            for token in data.split():
                if token.startswith("conn="):
                    conn_ids.append(token)
        except Exception as exc:
            ok = False
            conn_ids.append(f"error:{exc}")
            break
    conn.close()
    check(
        "upstream keepalive pool reuses connections",
        ok and len(conn_ids) == 3 and len(set(conn_ids)) == 1,
        f"conn ids={conn_ids}",
    )

    status, body, _ = http_request(proxy_port, "POST", "/post", body=b"hello-body")
    check("POST body forwarding", status == 200 and body == b"hello-body", f"{status} {body!r}")

    status, body, _ = http_request(proxy_port, "GET", "/api?x=1")
    check(
        "proxy_pass path replacement + query",
        status == 200 and b"request: /headers?x=1" in body,
        body[:200],
    )

    status, body, _ = http_request(proxy_port, "GET", "/chunk")
    check(
        "chunked response relay",
        status == 200 and body == b"chunk0\nchunk1\nchunk2\n",
        f"{status} {body!r}",
    )

    status, body, _ = http_request(proxy_port, "GET", "/missing")
    check("backend 404 passthrough", status == 404 and b"not found" in body, f"{status} {body!r}")

    status, body, _ = http_request(proxy_port, "GET", "/large?size=200000")
    check("large response", status == 200 and len(body) == 200000, f"{status} len={len(body)}")

    status, body, _ = http_request(proxy_port, "GET", "/hdr")
    lower = body.lower()
    check(
        "proxy_set_header and X-Real-IP/X-Forwarded-For",
        status == 200
        and b"x-test: yes" in lower
        and b"x-real-ip:" in lower
        and b"x-forwarded-for: 127.0.0.1" in lower,
        lower[:400],
    )

    status, body, _ = http_request(proxy_port, "GET", "/no-headers", headers={"X-Custom": "secret"})
    check(
        "proxy_pass_request_headers off",
        status == 200 and b"x-custom" not in body.lower() and b"x-forwarded-for:" in body.lower(),
        body[:400],
    )

    status, body, _ = http_request(proxy_port, "GET", "/default")
    check(
        "default content handler",
        status == 200 and b"Hello from RPS!" in body,
        f"{status} {body!r}",
    )

    status, body, _ = http_request(proxy_port, "GET", "/dead")
    check("dead upstream returns 502", status == 502, f"{status} {body!r}")

    ok, detail = websocket_test(proxy_port)
    check("WebSocket upgrade and echo", ok, detail)


def main():
    if not BIN.exists():
        print(f"binary not found: {BIN}")
        print("run `cargo build` first")
        return 1

    http_port = free_port()
    http_port2 = free_port()
    ws_port = free_port()
    proxy1 = free_port()
    proxy2 = free_port()
    proxy3 = free_port()
    tmp = Path(tempfile.mkdtemp(prefix="rps_it_", dir=ROOT / "test"))
    print(f"temp dir: {tmp}")

    try:
        start([PY, str(ROOT / "test" / "backend.py"), str(http_port)], tmp / "backend1.log")
        start([PY, str(ROOT / "test" / "backend.py"), str(http_port2)], tmp / "backend2.log")
        start([PY, str(ROOT / "test" / "ws_backend.py"), str(ws_port)], tmp / "ws.log")

        for port, name in ((http_port, "backend1"), (http_port2, "backend2"), (ws_port, "ws")):
            if not wait_port(port):
                print(f"failed to start {name} on {port}")
                return 1

        conf1 = tmp / "reactor.conf"
        conf1.write_text(
            config_text(proxy1, http_port, http_port2, ws_port, "epoll"),
            encoding="utf-8",
        )

        # -- config test -------------------------------------------------
        test_run = subprocess.run(
            [str(BIN), "-c", str(conf1), "-t"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
        )
        check(
            "-t validates the configuration",
            test_run.returncode == 0
            and "successful" in (test_run.stdout + test_run.stderr),
            test_run.stdout + test_run.stderr,
        )

        # -- reactor mode ------------------------------------------------
        rps1 = start(
            [str(BIN), "-c", str(conf1), "-l", "4"],
            tmp / "rps-reactor.log",
        )
        if not wait_port(proxy1):
            print("reactor RPS failed to start")
            print((tmp / "rps-reactor.log").read_text(errors="replace"))
            return 1
        run_suite(proxy1, "reactor / epoll")
        stop(rps1)

        # -- thread-per-connection mode ----------------------------------
        conf2 = tmp / "thread.conf"
        conf2.write_text(
            config_text(proxy2, http_port, http_port2, ws_port, "multithreading"),
            encoding="utf-8",
        )
        rps2 = start(
            [str(BIN), "-c", str(conf2), "-l", "4"],
            tmp / "rps-thread.log",
        )
        if not wait_port(proxy2):
            print("thread-mode RPS failed to start")
            print((tmp / "rps-thread.log").read_text(errors="replace"))
            return 1

        status, body, _ = http_request(proxy2, "GET", "/")
        check("thread mode: basic GET", status == 200 and b"backend " in body, f"{status} {body!r}")
        status, body, _ = http_request(proxy2, "POST", "/post", body=b"thread-body")
        check("thread mode: POST body", status == 200 and body == b"thread-body")
        status, body, _ = http_request(proxy2, "GET", "/chunk")
        check("thread mode: chunked response", status == 200 and b"chunk0" in body)
        ok, detail = websocket_test(proxy2)
        check("thread mode: WebSocket", ok, detail)
        stop(rps2)

        # -- daemon mode --------------------------------------------------
        conf3 = tmp / "daemon.conf"
        pid_path = tmp / "daemon.pid"
        conf3.write_text(
            config_text(proxy3, http_port, http_port2, ws_port, "epoll", daemon=True, pid=pid_path),
            encoding="utf-8",
        )
        daemon_parent = start(
            [str(BIN), "-c", str(conf3), "-l", "4"],
            tmp / "rps-daemon-parent.log",
        )
        try:
            daemon_parent.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        daemon_ok = wait_port(proxy3, timeout=10)
        if daemon_ok:
            status, body, _ = http_request(proxy3, "GET", "/")
            check("daemon mode: serves requests", status == 200 and b"backend " in body)
            stop_run = subprocess.run(
                [str(BIN), "-c", str(conf3), "-s", "stop"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=20,
            )
            stopped = wait_port(proxy3, timeout=10, open_=False) and not pid_path.exists()
            check(
                "daemon mode: -s stop",
                stop_run.returncode == 0 and stopped,
                f"rc={stop_run.returncode} stopped={stopped} "
                f"out={stop_run.stdout}{stop_run.stderr}",
            )
        else:
            check(
                "daemon mode: serves requests",
                False,
                "daemon did not open its port; see rps-daemon-parent.log",
            )

        print(f"\n=== result: {passed} passed, {failed} failed ===")
        return 0 if failed == 0 else 1
    finally:
        # Best-effort stop for a daemon that outlived a failed assertion.
        try:
            if "conf3" in locals() and "pid_path" in locals() and conf3.exists() and pid_path.exists():
                subprocess.run(
                    [str(BIN), "-c", str(conf3), "-s", "stop"],
                    capture_output=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=15,
                )
        except Exception:
            pass
        for proc in list(procs):
            stop(proc)
        for log in logs:
            try:
                log.close()
            except Exception:
                pass
        if failed == 0:
            shutil.rmtree(tmp, ignore_errors=True)
        else:
            print(f"logs kept in {tmp}")


if __name__ == "__main__":
    sys.exit(main())
