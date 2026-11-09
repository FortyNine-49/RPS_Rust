#!/usr/bin/env python3
"""Small HTTP/1.1 keep-alive backend used by the RPS integration tests."""

import socketserver
import sys
import threading
import time
from urllib.parse import parse_qs


class BackendServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr, handler):
        super().__init__(addr, handler)
        self.conn_counter = 0
        self.counter_lock = threading.Lock()
        self.port = addr[1]

    def next_connection_id(self):
        with self.counter_lock:
            self.conn_counter += 1
            return self.conn_counter


def read_chunked(rfile):
    body = b""
    while True:
        line = rfile.readline(65536)
        if not line:
            return body
        size = int(line.split(b";")[0].strip() or b"0", 16)
        if size == 0:
            while True:
                trailer = rfile.readline(65536)
                if trailer in (b"\r\n", b"\n", b""):
                    return body
        body += rfile.read(size)
        rfile.read(2)


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        server = self.server
        port = server.port
        conn_id = server.next_connection_id()
        request_count = 0

        while True:
            line = self.rfile.readline(65536)
            if not line:
                return
            if line in (b"\r\n", b"\n"):
                continue
            try:
                request_line = line.decode("latin1").rstrip("\r\n")
            except Exception:
                return
            parts = request_line.split(" ")
            if len(parts) < 3:
                return
            method, target, version = parts[0], parts[1], parts[2]

            headers = {}
            raw_headers = []
            while True:
                raw = self.rfile.readline(65536)
                if not raw:
                    return
                if raw in (b"\r\n", b"\n"):
                    break
                text = raw.decode("latin1").rstrip("\r\n")
                name, _, value = text.partition(":")
                name = name.strip()
                value = value.strip()
                raw_headers.append((name, value))
                headers[name.lower()] = value

            body = b""
            if headers.get("transfer-encoding", "").lower() == "chunked":
                body = read_chunked(self.rfile)
            else:
                length = int(headers.get("content-length", "0") or 0)
                if length:
                    body = self.rfile.read(length)

            request_count += 1
            keep = version == "HTTP/1.1" and headers.get("connection", "").lower() != "close"
            path, _, query = target.partition("?")
            query_args = parse_qs(query)
            status = "200 OK"
            extra = []

            if path == "/":
                payload = f"backend {port} conn={conn_id} req={request_count}\n".encode()
            elif path == "/headers":
                lines = [f"request: {target}"]
                lines += [f"{k}: {v}" for k, v in raw_headers]
                payload = ("\n".join(lines) + "\n").encode()
            elif path == "/echo":
                payload = body
            elif path == "/large":
                size = int(query_args.get("size", ["100000"])[0])
                payload = b"X" * size
            elif path == "/chunked":
                chunks = [b"chunk0\n", b"chunk1\n", b"chunk2\n"]
                self.wfile.write(b"HTTP/1.1 200 OK\r\n")
                self.wfile.write(b"Content-Type: text/plain\r\n")
                self.wfile.write(b"Transfer-Encoding: chunked\r\n")
                self.wfile.write(b"X-Backend: " + str(port).encode() + b"\r\n")
                self.wfile.write(b"Connection: keep-alive\r\n\r\n")
                for chunk in chunks:
                    self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
                if not keep:
                    return
                continue
            elif path.startswith("/status/"):
                code = path.rsplit("/", 1)[-1]
                status = f"{code} Status"
                payload = f"status {code}\n".encode()
            elif path == "/slow":
                delay = float(query_args.get("delay", ["1"])[0])
                time.sleep(delay)
                payload = f"delayed {delay}\n".encode()
            elif path == "/close":
                keep = False
                payload = b"closing\n"
            else:
                status = "404 Not Found"
                payload = b"not found\n"

            reason = status.split(" ", 1)[1] if " " in status else ""
            code = status.split(" ", 1)[0]
            self.wfile.write(f"HTTP/1.1 {code} {reason}\r\n".encode())
            self.wfile.write(b"Content-Type: text/plain\r\n")
            self.wfile.write(f"Content-Length: {len(payload)}\r\n".encode())
            self.wfile.write(b"X-Backend: " + str(port).encode() + b"\r\n")
            for name, value in extra:
                self.wfile.write(f"{name}: {value}\r\n".encode())
            if keep:
                self.wfile.write(b"Connection: keep-alive\r\n")
            else:
                self.wfile.write(b"Connection: close\r\n")
            self.wfile.write(b"\r\n")
            self.wfile.write(payload)
            self.wfile.flush()
            if not keep:
                return


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 19090
    server = BackendServer(("127.0.0.1", port), Handler)
    print(f"HTTP backend on 127.0.0.1:{port}", file=sys.stderr, flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
