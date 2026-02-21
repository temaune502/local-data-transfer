"""
transfer.py
───────────
TCP-based file and message transfer between peers.

Protocol (binary framing)
─────────────────────────
  [4 bytes] header length  (big-endian uint32)
  [N bytes] JSON header    (UTF-8)
  [M bytes] payload        (bytes, may be 0 for text messages)

Header fields (JSON):
  {
    "type":     "file" | "message",
    "name":     <filename>,          # type=='file' only
    "size":     <int bytes>,         # type=='file' only
    "text":     <str>,               # type=='message' only
    "sender":   <node_name>
  }
"""

import socket
import struct
import json
import os
import threading
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

CHUNK_SIZE = 64 * 1024      # 64 KiB read/write chunks
CONNECT_TIMEOUT = 5         # seconds


# ──────────────────────────────────────────────────────────────────────── #
#  Low-level framing helpers                                               #
# ──────────────────────────────────────────────────────────────────────── #

def _send_frame(sock: socket.socket, header: dict, payload: bytes = b""):
    raw_header = json.dumps(header).encode("utf-8")
    length_prefix = struct.pack(">I", len(raw_header))
    sock.sendall(length_prefix + raw_header + payload)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed while reading data.")
        buf.extend(chunk)
    return bytes(buf)


def _recv_header(sock: socket.socket) -> dict:
    raw_len = _recv_exact(sock, 4)
    header_len = struct.unpack(">I", raw_len)[0]
    raw_header = _recv_exact(sock, header_len)
    return json.loads(raw_header.decode("utf-8"))


# ──────────────────────────────────────────────────────────────────────── #
#  Sender side                                                             #
# ──────────────────────────────────────────────────────────────────────── #

def send_file(peer_ip: str, peer_port: int, filepath: str, sender_name: str,
              progress_cb=None):
    """
    Send a file to a remote peer.

    Parameters
    ----------
    peer_ip      : IP of the destination peer.
    peer_port    : TCP port of the destination peer's TransferServer.
    filepath     : Absolute or relative path to the file to send.
    sender_name  : Name of this node (shown on receiver side).
    progress_cb  : Optional callable(bytes_sent, total_bytes) for progress.

    Raises
    ------
    FileNotFoundError, ConnectionError, OSError
    """
    path = Path(filepath)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {filepath}")

    file_size = path.stat().st_size
    header = {
        "type": "file",
        "name": path.name,
        "size": file_size,
        "sender": sender_name,
    }

    with socket.create_connection((peer_ip, peer_port), timeout=CONNECT_TIMEOUT) as sock:
        # Send the header (no payload yet)
        raw_header = json.dumps(header).encode("utf-8")
        sock.sendall(struct.pack(">I", len(raw_header)) + raw_header)

        # Stream the file
        sent = 0
        with open(path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                sock.sendall(chunk)
                sent += len(chunk)
                if progress_cb:
                    progress_cb(sent, file_size)

    logger.info(f"File sent: {path.name} ({file_size} bytes) → {peer_ip}:{peer_port}")


def send_message(peer_ip: str, peer_port: int, text: str, sender_name: str):
    """
    Send a UTF-8 text message to a remote peer.

    Raises
    ------
    ConnectionError, OSError
    """
    header = {
        "type": "message",
        "text": text,
        "sender": sender_name,
    }
    with socket.create_connection((peer_ip, peer_port), timeout=CONNECT_TIMEOUT) as sock:
        _send_frame(sock, header)

    logger.info(f"Message sent to {peer_ip}:{peer_port}")


# ──────────────────────────────────────────────────────────────────────── #
#  Receiver side                                                           #
# ──────────────────────────────────────────────────────────────────────── #

class TransferServer:
    """
    Listens for incoming TCP connections from peers.

    Parameters
    ----------
    host         : Interface to bind to.  Usually '' or '0.0.0.0'.
    port         : TCP port to listen on.
    save_dir     : Directory where received files are saved.
    on_message   : Callable(sender_name, text) — called for text messages.
    on_file      : Callable(sender_name, filepath) — called after file saved.
    """

    def __init__(self,
                 host: str = "",
                 port: int = 9000,
                 save_dir: str = "received",
                 on_message=None,
                 on_file=None):
        self.host = host
        self.port = port
        self.save_dir = Path(save_dir)
        self.save_dir.mkdir(parents=True, exist_ok=True)

        self._on_message = on_message
        self._on_file = on_file

        self._running = False
        self._server_sock: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None

    def start(self):
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(10)
        self._server_sock.settimeout(1.0)

        self._running = True
        self._accept_thread = threading.Thread(
            target=self._accept_loop, name="TCPAccept", daemon=True
        )
        self._accept_thread.start()
        logger.info(f"TransferServer listening on port {self.port}")

    def stop(self):
        self._running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass

    def _accept_loop(self):
        while self._running:
            try:
                conn, addr = self._server_sock.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr[0]),
                    daemon=True,
                ).start()
            except socket.timeout:
                continue
            except OSError:
                if self._running:
                    logger.warning("Accept error on TransferServer.")
                break

    def _handle_connection(self, conn: socket.socket, sender_ip: str):
        try:
            with conn:
                header = _recv_header(conn)
                msg_type = header.get("type")
                sender_name = header.get("sender", sender_ip)

                if msg_type == "message":
                    text = header.get("text", "")
                    logger.info(f"Message from {sender_name}: {text}")
                    if self._on_message:
                        self._on_message(sender_name, text)

                elif msg_type == "file":
                    fname = os.path.basename(header.get("name", "unknown_file"))
                    fsize = int(header.get("size", 0))
                    save_path = self.save_dir / fname

                    # Avoid overwriting: append counter if needed
                    counter = 1
                    stem = save_path.stem
                    suffix = save_path.suffix
                    while save_path.exists():
                        save_path = self.save_dir / f"{stem}_{counter}{suffix}"
                        counter += 1

                    received = 0
                    with open(save_path, "wb") as f:
                        while received < fsize:
                            to_read = min(CHUNK_SIZE, fsize - received)
                            chunk = conn.recv(to_read)
                            if not chunk:
                                break
                            f.write(chunk)
                            received += len(chunk)

                    logger.info(
                        f"File received from {sender_name}: "
                        f"{save_path.name} ({received}/{fsize} bytes)"
                    )
                    if self._on_file:
                        self._on_file(sender_name, str(save_path))
                else:
                    logger.warning(f"Unknown packet type from {sender_ip}: {msg_type}")

        except Exception as exc:
            logger.debug(f"Connection error from {sender_ip}: {exc}")
