"""
transfer.py  v2
───────────────
Enhanced P2P transfer engine.

Features
────────
  • zlib compression per chunk (level 6)
  • Parallel chunk streams (MAX_PARALLEL simultaneous TCP connections)
  • SHA-256 integrity check: per-chunk  +  full-file
  • Folder support: auto-zipped before send, auto-extracted on receive
  • Dual-sided progress callbacks (sender & receiver)

Protocol  (binary framing – same as v1)
────────────────────────────────────────
  [4 bytes] header length  (big-endian uint32)
  [N bytes] JSON header    (UTF-8)
  [M bytes] payload        (raw bytes; absent for session_init / message)

Header types
────────────
  session_init   – opens a new transfer session on receiver
  chunk          – one compressed chunk belonging to a session
  message        – plain UTF-8 text (unchanged from v1)
"""

import hashlib
import json
import logging
import os
import shutil
import socket
import struct
import tempfile
import threading
import time
import uuid
import zipfile
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logger = logging.getLogger(__name__)

CHUNK_SIZE    = 2 * 1024 * 1024   # 2 MiB per chunk (uncompressed)
MAX_PARALLEL  = 4                  # simultaneous TCP connections for chunks
CONNECT_TIMEOUT = 10               # seconds


# ──────────────────────────────────────────────────────────────────────────── #
#  Framing helpers                                                             #
# ──────────────────────────────────────────────────────────────────────────── #

def _send_frame(sock: socket.socket, header: dict, payload: bytes = b""):
    raw = json.dumps(header).encode("utf-8")
    sock.sendall(struct.pack(">I", len(raw)) + raw + payload)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed while reading.")
        buf.extend(chunk)
    return bytes(buf)


def _recv_header(sock: socket.socket) -> dict:
    raw_len = _recv_exact(sock, 4)
    hlen = struct.unpack(">I", raw_len)[0]
    return json.loads(_recv_exact(sock, hlen).decode("utf-8"))


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fmt_size(n: int) -> str:
    """Human-readable byte size."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ──────────────────────────────────────────────────────────────────────────── #
#  Receiver: Session Buffer                                                    #
# ──────────────────────────────────────────────────────────────────────────── #

class _Session:
    """
    Accumulates incoming chunks for one file/folder transfer session.

    When every expected chunk has arrived:
      1. Reassembles data in order
      2. Verifies full-file SHA-256
      3. Saves to disk (extracts zip for folders)
      4. Calls on_complete callback
    """

    def __init__(self, *, session_id: str, filename: str, total_chunks: int,
                 original_size: int, full_sha256: str, save_dir: Path,
                 is_folder: bool, sender_name: str, on_complete, on_progress):
        self.session_id    = session_id
        self.filename      = filename
        self.total_chunks  = total_chunks
        self.original_size = original_size
        self.full_sha256   = full_sha256
        self.save_dir      = Path(save_dir)
        self.is_folder     = is_folder
        self.sender_name   = sender_name

        self._on_complete = on_complete
        self._on_progress = on_progress

        self._chunks: dict[int, bytes] = {}  # chunk_index → decompressed bytes
        self._received_bytes = 0
        self._lock = threading.Lock()
        self._created_at = time.time()

    # ── public ─────────────────────────────────────────────────────────── #

    def add_chunk(self, index: int, compressed: bytes, chunk_sha256: str):
        """Verify, decompress, store chunk; trigger finalization when all arrived."""
        # Per-chunk integrity
        actual = _sha256(compressed)
        if actual != chunk_sha256:
            raise ValueError(
                f"Chunk {index} SHA-256 mismatch!\n"
                f"  expected: {chunk_sha256[:12]}…\n"
                f"  got:      {actual[:12]}…"
            )

        decompressed = zlib.decompress(compressed)

        with self._lock:
            if index in self._chunks:
                return  # duplicate chunk – ignore
            self._chunks[index] = decompressed
            self._received_bytes += len(decompressed)
            count = len(self._chunks)
            rb    = self._received_bytes

        if self._on_progress:
            self._on_progress(
                min(rb, self.original_size),
                self.original_size,
                self.sender_name,
                self.filename,
            )

        if count == self.total_chunks:
            threading.Thread(target=self._finalize, daemon=True).start()

    # ── private ────────────────────────────────────────────────────────── #

    def _finalize(self):
        try:
            # Reassemble in order
            data = b"".join(self._chunks[i] for i in range(self.total_chunks))

            # Full-file integrity
            actual = _sha256(data)
            if actual != self.full_sha256:
                raise ValueError(
                    f"Full-file SHA-256 mismatch!\n"
                    f"  expected: {self.full_sha256[:16]}…\n"
                    f"  got:      {actual[:16]}…"
                )

            # Avoid overwrites
            out_path = _unique_path(self.save_dir / self.filename)

            with open(out_path, "wb") as f:
                f.write(data)

            elapsed = max(time.time() - self._created_at, 1e-6)
            speed   = self.original_size / elapsed

            if self.is_folder:
                extract_dir = _unique_path(self.save_dir / Path(self.filename).stem)
                extract_dir.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(out_path, "r") as zf:
                    zf.extractall(extract_dir)
                out_path.unlink()
                self._on_complete(
                    self.session_id, self.sender_name,
                    str(extract_dir), elapsed, speed, self.original_size,
                )
            else:
                self._on_complete(
                    self.session_id, self.sender_name,
                    str(out_path), elapsed, speed, self.original_size,
                )

        except Exception as exc:
            logger.error(f"[Session {self.session_id[:8]}] finalization error: {exc}")


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem, suffix = path.stem, path.suffix
    for i in range(1, 100_000):
        candidate = path.parent / f"{stem}_{i}{suffix}"
        if not candidate.exists():
            return candidate
    return path


# ──────────────────────────────────────────────────────────────────────────── #
#  Sender internals                                                            #
# ──────────────────────────────────────────────────────────────────────────── #

def _prepare_chunks(data: bytes) -> list[tuple[int, bytes, str, int]]:
    """
    Split raw bytes into CHUNK_SIZE pieces, compress each, compute SHA-256.

    Returns list of (chunk_index, compressed_bytes, sha256_hex, original_size).
    """
    result = []
    for i in range(0, max(len(data), 1), CHUNK_SIZE):
        raw   = data[i : i + CHUNK_SIZE]
        comp  = zlib.compress(raw, level=6)
        sha   = _sha256(comp)
        result.append((i // CHUNK_SIZE, comp, sha, len(raw)))
    return result


def _send_session_init(peer_ip: str, peer_port: int, header: dict):
    with socket.create_connection((peer_ip, peer_port), timeout=CONNECT_TIMEOUT) as sock:
        raw = json.dumps(header).encode("utf-8")
        sock.sendall(struct.pack(">I", len(raw)) + raw)


def _send_one_chunk(peer_ip: str, peer_port: int,
                    session_id: str, index: int,
                    compressed: bytes, sha256: str):
    header = {
        "type":            "chunk",
        "session_id":      session_id,
        "index":           index,
        "sha256":          sha256,
        "compressed_size": len(compressed),
    }
    with socket.create_connection((peer_ip, peer_port), timeout=CONNECT_TIMEOUT) as sock:
        raw = json.dumps(header).encode("utf-8")
        sock.sendall(struct.pack(">I", len(raw)) + raw + compressed)


# ──────────────────────────────────────────────────────────────────────────── #
#  Core send logic (shared by send_file / send_folder)                        #
# ──────────────────────────────────────────────────────────────────────────── #

def _send_data(peer_ip: str, peer_port: int, data: bytes,
               filename: str, sender_name: str,
               is_folder: bool, progress_cb=None):
    session_id = str(uuid.uuid4())
    total_size = len(data)
    full_sha   = _sha256(data)
    chunks     = _prepare_chunks(data)

    # ── 1. Announce session ──
    _send_session_init(peer_ip, peer_port, {
        "type":          "session_init",
        "session_id":    session_id,
        "sender":        sender_name,
        "filename":      filename,
        "original_size": total_size,
        "total_chunks":  len(chunks),
        "sha256":        full_sha,
        "is_folder":     is_folder,
    })

    # Small delay so receiver registers session before first chunk arrives
    time.sleep(0.15)

    # ── 2. Dispatch chunks in parallel ──
    bytes_sent = [0]
    lock       = threading.Lock()

    def dispatch(chunk_tuple: tuple):
        idx, comp, sha, orig = chunk_tuple
        _send_one_chunk(peer_ip, peer_port, session_id, idx, comp, sha)
        with lock:
            bytes_sent[0] += orig
            if progress_cb:
                progress_cb(min(bytes_sent[0], total_size), total_size)

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
        futures = {pool.submit(dispatch, c): c[0] for c in chunks}
        for f in as_completed(futures):
            exc = f.exception()
            if exc:
                raise exc

    logger.info(
        f"Sent {filename!r}  {fmt_size(total_size)}  "
        f"{len(chunks)} chunk(s)  MAX_PARALLEL={MAX_PARALLEL}  →  {peer_ip}:{peer_port}"
    )


# ──────────────────────────────────────────────────────────────────────────── #
#  Public Sender API                                                           #
# ──────────────────────────────────────────────────────────────────────────── #

def send_file(peer_ip: str, peer_port: int, filepath: str,
              sender_name: str, progress_cb=None):
    """
    Send a single file to a remote peer.

    Parameters
    ----------
    progress_cb : callable(bytes_sent, total_bytes) | None
    """
    path = Path(filepath)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {filepath}")
    data = path.read_bytes()
    _send_data(peer_ip, peer_port, data, path.name, sender_name,
               is_folder=False, progress_cb=progress_cb)


def send_folder(peer_ip: str, peer_port: int, folder_path: str,
                sender_name: str, progress_cb=None):
    """
    Zip a folder and send it.  The receiver auto-extracts the zip.

    Parameters
    ----------
    progress_cb : callable(bytes_sent, total_bytes) | None
    """
    folder = Path(folder_path)
    if not folder.is_dir():
        raise NotADirectoryError(f"Not a directory: {folder_path}")

    tmp = Path(tempfile.mkdtemp())
    zip_path = tmp / (folder.name + ".zip")
    try:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zf:
            for fp in folder.rglob("*"):
                if fp.is_file():
                    zf.write(fp, fp.relative_to(folder.parent))
        data = zip_path.read_bytes()
        _send_data(peer_ip, peer_port, data, zip_path.name, sender_name,
                   is_folder=True, progress_cb=progress_cb)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def send_message(peer_ip: str, peer_port: int, text: str, sender_name: str):
    """Send a plain UTF-8 text message (no chunking)."""
    header = {"type": "message", "text": text, "sender": sender_name}
    with socket.create_connection((peer_ip, peer_port), timeout=CONNECT_TIMEOUT) as sock:
        _send_frame(sock, header)


# ──────────────────────────────────────────────────────────────────────────── #
#  TransferServer                                                              #
# ──────────────────────────────────────────────────────────────────────────── #

class TransferServer:
    """
    Listens for incoming TCP connections and dispatches by message type:

      session_init  –  Opens a new _Session (file metadata)
      chunk         –  Delivers one compressed chunk to the correct session
      message       –  Plain text, calls on_message immediately

    Callbacks
    ---------
    on_message(sender_name, text)
    on_file(sender_name, path, elapsed_sec, speed_bytes_per_sec, total_bytes)
    on_receive_progress(bytes_received, total_bytes, sender_name, filename)
    """

    def __init__(self, host: str = "", port: int = 9000,
                 save_dir: str = "received",
                 on_message=None, on_file=None, on_receive_progress=None):
        self.host     = host
        self.port     = port
        self.save_dir = Path(save_dir)
        self.save_dir.mkdir(parents=True, exist_ok=True)

        self._on_message          = on_message
        self._on_file             = on_file
        self._on_receive_progress = on_receive_progress

        self._sessions: dict[str, _Session] = {}
        self._sessions_lock = threading.Lock()

        self._running = False
        self._server_sock: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None

    # ── public ─────────────────────────────────────────────────────────── #

    def start(self):
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(64)   # large backlog: many parallel chunk conns
        self._server_sock.settimeout(1.0)

        self._running = True
        self._accept_thread = threading.Thread(
            target=self._accept_loop, name="TCPAccept", daemon=True
        )
        self._accept_thread.start()
        logger.info(f"TransferServer listening on :{self.port}")

    def stop(self):
        self._running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass

    # ── internal ───────────────────────────────────────────────────────── #

    def _accept_loop(self):
        while self._running:
            try:
                conn, addr = self._server_sock.accept()
                threading.Thread(
                    target=self._handle,
                    args=(conn, addr[0]),
                    daemon=True,
                ).start()
            except socket.timeout:
                continue
            except OSError:
                if self._running:
                    logger.warning("Accept error on TransferServer.")
                break

    def _handle(self, conn: socket.socket, sender_ip: str):
        try:
            with conn:
                header   = _recv_header(conn)
                msg_type = header.get("type")

                if   msg_type == "message":      self._h_message(header, sender_ip)
                elif msg_type == "session_init": self._h_session_init(header, sender_ip)
                elif msg_type == "chunk":        self._h_chunk(conn, header, sender_ip)
                else:
                    logger.warning(
                        f"Unknown packet type {msg_type!r} from {sender_ip}"
                    )
        except Exception as exc:
            logger.debug(f"Connection error from {sender_ip}: {exc}")

    def _h_message(self, header: dict, sender_ip: str):
        sender = header.get("sender", sender_ip)
        text   = header.get("text", "")
        if self._on_message:
            self._on_message(sender, text)

    def _h_session_init(self, header: dict, sender_ip: str):
        sid = header["session_id"]
        session = _Session(
            session_id    = sid,
            filename      = os.path.basename(header.get("filename", "file")),
            total_chunks  = int(header["total_chunks"]),
            original_size = int(header["original_size"]),
            full_sha256   = header["sha256"],
            save_dir      = self.save_dir,
            is_folder     = bool(header.get("is_folder", False)),
            sender_name   = header.get("sender", sender_ip),
            on_complete   = self._on_session_complete,
            on_progress   = self._on_receive_progress,
        )
        with self._sessions_lock:
            self._sessions[sid] = session
        logger.debug(f"Session registered: {sid[:8]}…  {session.filename}")

    def _h_chunk(self, conn: socket.socket, header: dict, sender_ip: str):
        sid             = header["session_id"]
        index           = int(header["index"])
        compressed_size = int(header["compressed_size"])
        chunk_sha256    = header["sha256"]

        compressed = _recv_exact(conn, compressed_size)

        with self._sessions_lock:
            session = self._sessions.get(sid)

        if session is None:
            logger.warning(
                f"Chunk #{index} for unknown session {sid[:8]}… from {sender_ip}"
            )
            return

        session.add_chunk(index, compressed, chunk_sha256)

    def _on_session_complete(self, session_id: str, sender_name: str,
                              path: str, elapsed: float, speed: float, size: int):
        # Remove session record
        with self._sessions_lock:
            self._sessions.pop(session_id, None)
        # Notify application layer
        if self._on_file:
            self._on_file(sender_name, path, elapsed, speed, size)
