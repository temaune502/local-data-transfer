"""
transfer.py  v5
───────────────
Enhanced P2P transfer engine.

Memory model
────────────
  Sender  : reads file in CHUNK_SIZE slices; at most MAX_PARALLEL*2 compressed
            chunks live in RAM at once (bounded by a Semaphore). The entire
            file is never loaded into memory.
  Receiver: decompressed chunks are written to per-session temp files;
            the in-memory dict is replaced by a set of received indices.
            Finalization streams from temp files – only one chunk in RAM
            at a time.

Features
────────
  • zlib compression per chunk (level 6)
  • Parallel chunk streams (MAX_PARALLEL TCP connections)
  • SHA-256: per-chunk integrity + full-file integrity (streaming on sender)
  • Folder: walk files, stream ALL chunks in parallel (no ZIP)
  • Dual-sided progress callbacks

Protocol
────────
  [4B uint32 big-endian] header length
  [N bytes UTF-8 JSON]   header
  [M bytes]              payload (chunks only)

  session_init  { type, session_id, sender, rel_path,
                  original_size, total_chunks, sha256 }
  chunk         { type, session_id, index, sha256, compressed_size }
                + payload
  message       { type, sender, text }
"""

import hashlib
import json
import logging
import math
import os
import shutil
import socket
import struct
import tempfile
import threading
import time
import uuid
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logger = logging.getLogger(__name__)

CHUNK_SIZE      = 2 * 1024 * 1024   # 2 MiB per chunk (uncompressed)
MAX_PARALLEL    = 4                  # simultaneous TCP chunk connections
# At most MAX_PARALLEL * 2 compressed chunks live in RAM simultaneously.
# Peak sender RAM ≈ MAX_PARALLEL * 2 * CHUNK_SIZE * compression_ratio ≈ 8 MB
_MAX_INFLIGHT   = MAX_PARALLEL * 2
CONNECT_TIMEOUT = 10                 # seconds


# ──────────────────────────────────────────────────────────────────────────── #
#  Utilities                                                                   #
# ──────────────────────────────────────────────────────────────────────────── #

def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    """Stream a file and return its SHA-256 without loading it entirely."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(1 << 20)   # 1 MiB read window
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem, suffix = path.stem, path.suffix
    for i in range(1, 100_000):
        cand = path.parent / f"{stem}_{i}{suffix}"
        if not cand.exists():
            return cand
    return path


def _sanitise_rel_path(raw: str) -> str:
    """
    Normalise a relative path received over the network.
    Strips leading slashes, dots, and any path-traversal components.
    """
    parts = []
    for part in raw.replace("\\", "/").split("/"):
        part = part.strip()
        if part and part != "." and part != "..":
            parts.append(part)
    return "/".join(parts) if parts else "unknown_file"


# ──────────────────────────────────────────────────────────────────────────── #
#  Framing                                                                     #
# ──────────────────────────────────────────────────────────────────────────── #

def _send_frame(sock: socket.socket, header: dict, payload: bytes = b""):
    raw = json.dumps(header).encode()
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
    hlen = struct.unpack(">I", _recv_exact(sock, 4))[0]
    return json.loads(_recv_exact(sock, hlen).decode())


# ──────────────────────────────────────────────────────────────────────────── #
#  Receiver: Session                                                           #
# ──────────────────────────────────────────────────────────────────────────── #

class _Session:
    """
    Accumulates chunks for one file transfer on the receiver side.

    Each decompressed chunk is immediately written to a temp file so that
    RAM usage stays bounded regardless of file size.  Finalization streams
    from those temp files, verifies the full-file SHA-256, and writes the
    final output – at most one chunk in RAM at a time.
    """

    def __init__(self, *, session_id: str, rel_path: str,
                 total_chunks: int, original_size: int, full_sha256: str,
                 save_dir: Path, sender_name: str, on_complete, on_progress):
        self.session_id    = session_id
        self.rel_path      = rel_path
        self.display_name  = Path(rel_path).name
        self.total_chunks  = total_chunks
        self.original_size = original_size
        self.full_sha256   = full_sha256
        self.save_dir      = save_dir
        self.sender_name   = sender_name
        self._on_complete  = on_complete
        self._on_progress  = on_progress

        # Received chunk indices (lightweight, just ints)
        self._received: set[int] = set()
        self._recv_bytes = 0
        self._lock       = threading.Lock()
        self._created_at = time.time()

        # Each chunk is stored as a separate temp file: <tmp_dir>/<index:08d>.chunk
        self._tmp_dir = Path(tempfile.mkdtemp(prefix="ldt_rx_"))

    # ── public ─────────────────────────────────────────────────────────── #

    def add_chunk(self, index: int, compressed: bytes, chunk_sha256: str):
        # Per-chunk integrity
        actual = _sha256_bytes(compressed)
        if actual != chunk_sha256:
            raise ValueError(
                f"Chunk {index} SHA-256 mismatch for {self.display_name}\n"
                f"  expected {chunk_sha256[:12]}…  got {actual[:12]}…"
            )

        # Deduplicate before decompression
        with self._lock:
            if index in self._received:
                return
            self._received.add(index)

        # Decompress and persist to temp file (outside the lock – CPU+IO)
        decompressed = zlib.decompress(compressed)
        chunk_file = self._tmp_dir / f"{index:08d}.chunk"
        chunk_file.write_bytes(decompressed)

        with self._lock:
            self._recv_bytes += len(decompressed)
            count = len(self._received)
            rb    = self._recv_bytes

        if self._on_progress:
            self._on_progress(
                min(rb, self.original_size), self.original_size,
                self.sender_name, self.display_name,
            )

        if count == self.total_chunks:
            threading.Thread(target=self._finalize, daemon=True).start()

    # ── private ────────────────────────────────────────────────────────── #

    def _finalize(self):
        out_path = None
        try:
            out_path = _unique_path(self.save_dir / Path(self.rel_path))
            out_path.parent.mkdir(parents=True, exist_ok=True)

            # Stream temp chunks → output file, computing SHA-256 incrementally
            hasher = hashlib.sha256()
            with open(out_path, "wb") as out_f:
                for i in range(self.total_chunks):
                    chunk_data = (self._tmp_dir / f"{i:08d}.chunk").read_bytes()
                    hasher.update(chunk_data)
                    out_f.write(chunk_data)

            # Full-file integrity check
            actual = hasher.hexdigest()
            if actual != self.full_sha256:
                out_path.unlink(missing_ok=True)
                raise ValueError(
                    f"Full-file SHA-256 mismatch for {self.display_name}!\n"
                    f"  expected {self.full_sha256[:20]}…\n"
                    f"  got      {actual[:20]}…"
                )

            elapsed = max(time.time() - self._created_at, 1e-6)
            speed   = self.original_size / elapsed
            self._on_complete(
                self.session_id, self.sender_name,
                str(out_path), elapsed, speed, self.original_size,
            )

        except Exception as exc:
            logger.error(f"[Session {self.session_id[:8]}] finalize error: {exc}")
        finally:
            shutil.rmtree(self._tmp_dir, ignore_errors=True)


# ──────────────────────────────────────────────────────────────────────────── #
#  Sender internals                                                            #
# ──────────────────────────────────────────────────────────────────────────── #

def _do_send_session_init(peer_ip: str, peer_port: int, header: dict):
    with socket.create_connection((peer_ip, peer_port), timeout=CONNECT_TIMEOUT) as s:
        raw = json.dumps(header).encode()
        s.sendall(struct.pack(">I", len(raw)) + raw)


def _do_send_chunk(peer_ip: str, peer_port: int,
                   session_id: str, index: int,
                   compressed: bytes, sha256: str):
    header = {
        "type":            "chunk",
        "session_id":      session_id,
        "index":           index,
        "sha256":          sha256,
        "compressed_size": len(compressed),
    }
    with socket.create_connection((peer_ip, peer_port), timeout=CONNECT_TIMEOUT) as s:
        raw = json.dumps(header).encode()
        s.sendall(struct.pack(">I", len(raw)) + raw + compressed)


# ──────────────────────────────────────────────────────────────────────────── #
#  File task metadata (no file content stored)                                 #
# ──────────────────────────────────────────────────────────────────────────── #

class _FileTask:
    """
    Lightweight descriptor for one file to send.

    The file content is NOT loaded here.  SHA-256 is computed in a single
    streaming pass.  Actual chunk data is read lazily during dispatch.
    """
    __slots__ = ("session_id", "rel_path", "path",
                 "file_size", "total_chunks", "full_sha")

    def __init__(self, rel_path: str, path: Path):
        self.session_id   = str(uuid.uuid4())
        self.rel_path     = rel_path
        self.path         = path
        self.file_size    = path.stat().st_size
        self.total_chunks = (
            max(1, math.ceil(self.file_size / CHUNK_SIZE))
            if self.file_size > 0 else 1
        )
        # One streaming pass – no file content kept in RAM
        self.full_sha = _sha256_file(path)


# ──────────────────────────────────────────────────────────────────────────── #
#  Core dispatch (streaming, bounded memory)                                   #
# ──────────────────────────────────────────────────────────────────────────── #

def _dispatch_tasks(peer_ip: str, peer_port: int, tasks: list,
                    sender_name: str, total_bytes: int, progress_cb=None):
    """
    1. Send session_init for every task (small, fast).
    2. Stream each file's chunks in parallel, bounded by a semaphore so that
       at most _MAX_INFLIGHT compressed chunks exist in RAM simultaneously.
    """
    # ── 1. Announce all sessions ──
    for t in tasks:
        _do_send_session_init(peer_ip, peer_port, {
            "type":          "session_init",
            "session_id":    t.session_id,
            "sender":        sender_name,
            "rel_path":      t.rel_path,
            "original_size": t.file_size,
            "total_chunks":  t.total_chunks,
            "sha256":        t.full_sha,
        })
        # Small pause per session; receiver pending-buffer handles any race.
        time.sleep(0.04)

    # ── 2. Bounded-memory parallel chunk streaming ──
    sent = [0]
    lock     = threading.Lock()
    inflight = threading.Semaphore(_MAX_INFLIGHT)   # bounds RAM

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
        futures = []

        for t in tasks:
            if t.file_size == 0:
                # Empty file: send one empty compressed chunk
                inflight.acquire()
                comp = zlib.compress(b"", level=6)
                sha  = _sha256_bytes(comp)

                def _empty(sid=t.session_id, c=comp, s=sha):
                    try:
                        _do_send_chunk(peer_ip, peer_port, sid, 0, c, s)
                    finally:
                        inflight.release()

                futures.append(pool.submit(_empty))
            else:
                with open(t.path, "rb") as f:
                    idx = 0
                    while True:
                        inflight.acquire()
                        raw = f.read(CHUNK_SIZE)
                        if not raw:
                            inflight.release()
                            break
                        comp = zlib.compress(raw, level=6)
                        sha  = _sha256_bytes(comp)
                        orig = len(raw)

                        def _chunk(sid=t.session_id, i=idx,
                                   c=comp, s=sha, o=orig):
                            try:
                                _do_send_chunk(peer_ip, peer_port, sid, i, c, s)
                                with lock:
                                    sent[0] = min(sent[0] + o, total_bytes)
                                    if progress_cb:
                                        progress_cb(sent[0], total_bytes)
                            finally:
                                inflight.release()

                        futures.append(pool.submit(_chunk))
                        idx += 1

        for fut in as_completed(futures):
            exc = fut.exception()
            if exc:
                raise exc


# ──────────────────────────────────────────────────────────────────────────── #
#  Public Sender API                                                           #
# ──────────────────────────────────────────────────────────────────────────── #

def send_file(peer_ip: str, peer_port: int, filepath: str,
              sender_name: str, progress_cb=None):
    """
    Send a single file using streaming I/O.
    RAM usage: O(MAX_PARALLEL * CHUNK_SIZE) regardless of file size.
    progress_cb(bytes_sent, total_bytes)
    """
    path = Path(filepath)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {filepath}")
    task = _FileTask(path.name, path)
    _dispatch_tasks(peer_ip, peer_port, [task], sender_name,
                    task.file_size, progress_cb)
    logger.info(f"File sent: {path.name}  {fmt_size(task.file_size)}")


def send_folder(peer_ip: str, peer_port: int, folder_path: str,
                sender_name: str, progress_cb=None):
    """
    Send an entire folder using streaming I/O per file.
    Each file is chunked, compressed, and streamed in parallel.
    The receiver recreates the full folder tree under received/<folder_name>/.
    progress_cb(bytes_sent, total_bytes)
    """
    folder = Path(folder_path)
    if not folder.is_dir():
        raise NotADirectoryError(f"Not a directory: {folder_path}")

    all_files = sorted(f for f in folder.rglob("*") if f.is_file())
    if not all_files:
        raise ValueError(f"Folder is empty: {folder_path}")

    tasks = []
    for file in all_files:
        # rel_path preserves folder name as first component
        rel = "/".join(file.relative_to(folder.parent).parts)
        tasks.append(_FileTask(rel, file))

    total_bytes = sum(t.file_size for t in tasks)
    logger.info(
        f"Sending folder {folder.name!r}  "
        f"{len(tasks)} file(s)  {fmt_size(total_bytes)}"
    )
    _dispatch_tasks(peer_ip, peer_port, tasks, sender_name,
                    total_bytes, progress_cb)
    logger.info(f"Folder sent: {folder.name}")


def send_message(peer_ip: str, peer_port: int, text: str, sender_name: str):
    """Send a plain UTF-8 text message."""
    header = {"type": "message", "text": text, "sender": sender_name}
    with socket.create_connection((peer_ip, peer_port), timeout=CONNECT_TIMEOUT) as s:
        _send_frame(s, header)


# ──────────────────────────────────────────────────────────────────────────── #
#  TransferServer                                                              #
# ──────────────────────────────────────────────────────────────────────────── #

class TransferServer:
    """
    TCP server that handles incoming transfers.

    Dispatches by header "type":
      session_init  → registers a new _Session (or replays buffered chunks)
      chunk         → delivers one chunk to the session (or buffers it)
      message       → calls on_message immediately

    Race-condition safety
    ─────────────────────
    Chunks that arrive before their session_init are stored in _pending and
    replayed the moment the session is registered.  No timing assumptions.

    Callbacks
    ---------
    on_message(sender, text)
    on_file(sender, path, elapsed_sec, speed_bps, total_bytes)
    on_receive_progress(bytes_recv, total_bytes, sender, filename)
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

        self._sessions: dict[str, _Session]                    = {}
        self._pending:  dict[str, list[tuple[int, bytes, str]]] = {}
        self._sess_lock = threading.Lock()

        self._running = False
        self._sock: socket.socket | None    = None
        self._thread: threading.Thread | None = None

    # ── lifecycle ──────────────────────────────────────────────────────── #

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(64)
        self._sock.settimeout(1.0)
        self._running = True
        self._thread  = threading.Thread(
            target=self._accept_loop, name="TCPAccept", daemon=True
        )
        self._thread.start()
        logger.info(f"TransferServer listening on :{self.port}")

    def stop(self):
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        # Clean up any orphaned temp dirs from incomplete sessions
        with self._sess_lock:
            for session in self._sessions.values():
                shutil.rmtree(session._tmp_dir, ignore_errors=True)
            self._sessions.clear()
            self._pending.clear()

    # ── internal ───────────────────────────────────────────────────────── #

    def _accept_loop(self):
        while self._running:
            try:
                conn, addr = self._sock.accept()
                threading.Thread(
                    target=self._handle, args=(conn, addr[0]), daemon=True
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
                hdr = _recv_header(conn)
                t   = hdr.get("type")
                if   t == "message":      self._h_message(hdr, sender_ip)
                elif t == "session_init": self._h_session_init(hdr, sender_ip)
                elif t == "chunk":        self._h_chunk(conn, hdr, sender_ip)
                else:
                    logger.warning(f"Unknown type {t!r} from {sender_ip}")
        except Exception as exc:
            logger.debug(f"Connection error from {sender_ip}: {exc}")

    def _h_message(self, hdr: dict, sender_ip: str):
        if self._on_message:
            self._on_message(hdr.get("sender", sender_ip), hdr.get("text", ""))

    def _h_session_init(self, hdr: dict, sender_ip: str):
        sid      = hdr["session_id"]
        rel_path = _sanitise_rel_path(
            hdr.get("rel_path", hdr.get("filename", "unknown_file"))
        )

        session = _Session(
            session_id    = sid,
            rel_path      = rel_path,
            total_chunks  = int(hdr["total_chunks"]),
            original_size = int(hdr["original_size"]),
            full_sha256   = hdr["sha256"],
            save_dir      = self.save_dir,
            sender_name   = hdr.get("sender", sender_ip),
            on_complete   = self._on_done,
            on_progress   = self._on_receive_progress,
        )

        with self._sess_lock:
            self._sessions[sid] = session
            buffered = self._pending.pop(sid, [])

        tag = f"  (replaying {len(buffered)} buffered chunk(s))" if buffered else ""
        logger.debug(f"Session registered {sid[:8]}…  {rel_path}{tag}")

        # Replay early-arriving chunks (outside lock – CPU/IO work)
        for b_idx, b_comp, b_sha in buffered:
            try:
                session.add_chunk(b_idx, b_comp, b_sha)
            except Exception as exc:
                logger.error(
                    f"Buffered chunk error  session={sid[:8]}  idx={b_idx}: {exc}"
                )

    def _h_chunk(self, conn: socket.socket, hdr: dict, sender_ip: str):
        sid  = hdr["session_id"]
        idx  = int(hdr["index"])
        size = int(hdr["compressed_size"])
        sha  = hdr["sha256"]

        compressed = _recv_exact(conn, size)   # read payload before acquiring lock

        with self._sess_lock:
            session = self._sessions.get(sid)
            if session is None:
                # Session not registered yet – buffer chunk for later replay
                self._pending.setdefault(sid, []).append((idx, compressed, sha))
                logger.debug(
                    f"Chunk #{idx} buffered  session={sid[:8]}… not yet registered"
                )
                return

        # Deliver outside the lock (CPU/IO intensive)
        try:
            session.add_chunk(idx, compressed, sha)
        except Exception as exc:
            logger.error(f"add_chunk error  session={sid[:8]}  idx={idx}: {exc}")

    def _on_done(self, session_id: str, sender_name: str,
                 path: str, elapsed: float, speed: float, size: int):
        with self._sess_lock:
            self._sessions.pop(session_id, None)
        if self._on_file:
            self._on_file(sender_name, path, elapsed, speed, size)
