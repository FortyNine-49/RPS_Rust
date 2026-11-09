#!/usr/bin/env python3
"""Minimal WebSocket echo backend used by the RPS integration tests."""

import base64
import hashlib
import socketserver
import struct
import sys

WS_MAGIC = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def ws_accept_key(client_key: str) -> str:
    digest = hashlib.sha1(client_key.encode() + WS_MAGIC).digest()
    return base64.b64encode(digest).decode()


def recv_exact(rfile, n):
    data = b""
    while len(data) < n:
        chunk = rfile.read(n - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def recv_frame(rfile):
    head = recv_exact(rfile, 2)
    if head is None:
        return None, None
    b1, b2 = head[0], head[1]
    opcode = b1 & 0x0F
    masked = (b2 & 0x80) != 0
    length = b2 & 0x7F
    if length == 126:
        ext = recv_exact(rfile, 2)
        if ext is None:
            return None, None
        length = struct.unpack(">H", ext)[0]
    elif length == 127:
        ext = recv_exact(rfile, 8)
        if ext is None:
            return None, None
        length = struct.unpack(">Q", ext)[0]
    mask = recv_exact(rfile, 4) if masked else b""
    if masked and mask is None:
        return None, None
    payload = recv_exact(rfile, length)
    if payload is None:
        return None, None
    if masked:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def send_frame(wfile, opcode, payload: bytes):
    header = bytes([0x80 | opcode])
    length = len(payload)
    if length < 126:
        header += bytes([length])
    elif length < 65536:
        header += bytes([126]) + struct.pack(">H", length)
    else:
        header += bytes([127]) + struct.pack(">Q", length)
    wfile.write(header + payload)
    wfile.flush()


class WSServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        request_line = self.rfile.readline(65536)
        if not request_line:
            return
        headers = {}
        while True:
            line = self.rfile.readline(65536)
            if not line or line in (b"\r\n", b"\n"):
                break
            text = line.decode("latin1").rstrip("\r\n")
            name, _, value = text.partition(":")
            headers[name.strip().lower()] = value.strip()

        key = headers.get("sec-websocket-key")
        if not key:
            self.wfile.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            return

        accept = ws_accept_key(key)
        self.wfile.write(
            (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n"
                "\r\n"
            ).encode()
        )
        self.wfile.flush()

        while True:
            opcode, payload = recv_frame(self.rfile)
            if opcode is None:
                return
            if opcode == 0x8:  # close
                send_frame(self.wfile, 0x8, payload[:2])
                return
            if opcode == 0x9:  # ping
                send_frame(self.wfile, 0xA, payload)
                continue
            if opcode == 0xA:  # pong
                continue
            if opcode in (0x1, 0x2):
                send_frame(self.wfile, opcode, payload)


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 19092
    server = WSServer(("127.0.0.1", port), Handler)
    print(f"WS backend on 127.0.0.1:{port}", file=sys.stderr, flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
