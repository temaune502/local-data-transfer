"""
transfer.py  v4
───────────────
Enhanced P2P transfer engine.

Features
────────
  • zlib compression per chunk (level 6)
  • Parallel chunk streams over MAX_PARALLEL simultaneous TCP connections
  • SHA-256 integrity: per-chunk  +  full-file
  • Single-file: split → compress → stream in parallel
  • Folder: walk files, split ALL chunks from ALL files, stream ALL in parallel
    (no ZIP archiving – direct chunked streaming per file)
  • Dual-sided progress callbacks (sender & receiver)

Protocol (binary framing)
──────────────────────────
  [4B] header length  (big-endian uint32)
  [NB] JSON header    (UTF-8)
  [MB] payload        (absent for session_init / message)

Header types
────────────
  session_init  – opens a new file transfer session on receiver
  chunk         – one compressed chunk for a session
  message       – plain UTF-8 text

session_init fields
───────────────────
  {
    "type":          "session_init",
    "session_id":    <uuid4 str>,
    "sender":        <node name>,
    "rel_path":      <forward-slash relative save path>,  e.g. "folder/sub/file.txt"
    "original_size": <int bytes>,
    "total_chunks":  <int>,
    "sha256":        <hex str – full file>
  }

chunk fields
────────────
  {
    "type":            "chunk",
    "session_id":      <uuid4 str>,
    "index":           <int>,
    "sha256":          <hex str – compressed chunk>,
    "compressed_size": <int>
  }
  + payload: raw compressed bytes
"""

import hashlib
import json
import logging
import os
import socket
import struct
import threading
import time
import uuid
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logger = logging.getLogger(__name__)

CHUNK_SIZE     = 2 * 1024 * 1024   # 2 MiB uncompressed per chunk
MAX_PARALLEL   = 4                  # simultaneous TCP connections
CONNECT_TIMEOUT = 10               # seconds


# ──────────────────────────────────────────────────────────────────────────── #
#  Utilities                                                                   #
# ──────────────────────────────────────────────────────────────────────────── #

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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
#  Chunking                                                                    #
# ──────────────────────────────────────────────────────────────────────────── #

# Chunk tuple: (index, compressed_bytes, sha256_of_compressed, original_size)
ChunkTuple = tuple[int, bytes, str, int]


def _prepare_chunks(data: bytes) -> list[ChunkTuple]:
    """Split, compress, hash.  Returns list of (idx, compressed, sha, orig_size)."""
    result: list[ChunkTuple] = []
    total = len(data)
    for i in range(0, max(total, 1), CHUNK_SIZE):
        raw  = data[i : i + CHUNK_SIZE]
        comp = zlib.compress(raw, level=6)
        result.append((i // CHUNK_SIZE, comp, _sha256(comp), len(raw)))
    return result


# ──────────────────────────────────────────────────────────────────────────── #
#  Receiver: Session Buffer                                                    #
# ──────────────────────────────────────────────────────────────────────────── #

class _Session:
    """
    Accumulates compressed chunks for one file.
    On completion: decompresses + reassembles + verifies SHA-256 + saves.
    """

    def __init__(self, *, session_id: str, rel_path: str,
                 total_chunks: int, original_size: int, full_sha256: str,
                 save_dir: Path, sender_name: str, on_complete, on_progress):
        self.session_id    = session_id
        self.rel_path      = rel_path          # forward-slash relative path
        self.display_name  = Path(rel_path).name
        self.total_chunks  = total_chunks
        self.original_size = original_size
        self.full_sha256   = full_sha256
        self.save_dir      = save_dir
        self.sender_name   = sender_name
        self._on_complete  = on_complete
        self._on_progress  = on_progress

        self._chunks: dict[int, bytes] = {}
        self._recv_bytes  = 0
        self._lock        = threading.Lock()
        self._created_at  = time.time()

    def add_chunk(self, index: int, compressed: bytes, chunk_sha256: str):
        # Per-chunk integrity
        actual = _sha256(compressed)
        if actual != chunk_sha256:
            raise ValueError(
                f"Chunk {index} SHA-256 mismatch for {self.display_name}\n"
                f"  expected {chunk_sha256[:12]}…  got {actual[:12]}…"
            )

        decompressed = zlib.decompress(compressed)

        with self._lock:
            if index in self._chunks:
                return                    # duplicate – skip
            self._chunks[index] = decompressed
            self._recv_bytes += len(decompressed)
            count = len(self._chunks)
            rb    = self._recv_bytes

        if self._on_progress:
            self._on_progress(
                min(rb, self.original_size), self.original_size,
                self.sender_name, self.display_name,
            )

        if count == self.total_chunks:
            threading.Thread(target=self._finalize, daemon=True).start()

    def _finalize(self):
        try:
            data = b"".join(self._chunks[i] for i in range(self.total_chunks))

            actual = _sha256(data)
            if actual != self.full_sha256:
                raise ValueError(
                    f"Full-file SHA-256 mismatch for {self.display_name}!\n"
                    f"  expected {self.full_sha256[:16]}…  got {actual[:16]}…"
                )

            # Build output path, preserving folder structure
            out = _unique_path(self.save_dir / Path(self.rel_path))
            out.parent.mkdir(parents=True, exist_ok=True)

            with open(out, "wb") as f:
                f.write(data)

            elapsed = max(time.time() - self._created_at, 1e-6)
            speed   = self.original_size / elapsed
            self._on_complete(
                self.session_id, self.sender_name,
                str(out), elapsed, speed, self.original_size,
            )

        except Exception as exc:
            logger.error(f"[Session {self.session_id[:8]}] finalize error: {exc}")


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
#  Core: send one logical data block (used by both file and folder paths)      #
# ──────────────────────────────────────────────────────────────────────────── #

class _FileTask:
    """Everything needed to send one file as a session."""
    __slots__ = ("session_id", "rel_path", "data", "full_sha", "chunks")

    def __init__(self, rel_path: str, data: bytes):
        self.session_id = str(uuid.uuid4())
        self.rel_path   = rel_path
        self.data       = data
        self.full_sha   = _sha256(data)
        self.chunks     = _prepare_chunks(data)


def _dispatch_tasks(peer_ip: str, peer_port: int, tasks: list[_FileTask],
                    sender_name: str, total_bytes: int, progress_cb=None):
    """
    1. Send session_init for every task.
    2. Dispatch ALL chunks from ALL tasks in parallel (MAX_PARALLEL workers).
    """
    # ── 1. Announce all sessions ──
    for t in tasks:
        _do_send_session_init(peer_ip, peer_port, {
            "type":          "session_init",
            "session_id":    t.session_id,
            "sender":        sender_name,
            "rel_path":      t.rel_path,
            "original_size": len(t.data),
            "total_chunks":  len(t.chunks),
            "sha256":        t.full_sha,
        })
        time.sleep(0.05)        # give receiver a moment per session_init

    # ── 2. Flatten all chunks, dispatch in parallel ──
    # NOTE: no sleep needed – receiver buffers chunks that arrive before
    #       session_init and replays them automatically.
    work = []                   # (session_id, idx, comp, sha, orig_size)
    for t in tasks:
        for idx, comp, sha, orig in t.chunks:
            work.append((t.session_id, idx, comp, sha, orig))

    sent = [0]
    lock = threading.Lock()

    def dispatch(item):
        sid, idx, comp, sha, orig = item
        _do_send_chunk(peer_ip, peer_port, sid, idx, comp, sha)
        with lock:
            sent[0] = min(sent[0] + orig, total_bytes)
            if progress_cb:
                progress_cb(sent[0], total_bytes)

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
        futures = [pool.submit(dispatch, w) for w in work]
        for f in as_completed(futures):
            exc = f.exception()
            if exc:
                raise exc


# ──────────────────────────────────────────────────────────────────────────── #
#  Public Sender API                                                           #
# ──────────────────────────────────────────────────────────────────────────── #

def send_file(peer_ip: str, peer_port: int, filepath: str,
              sender_name: str, progress_cb=None):
    """
    Send a single file.
    progress_cb(bytes_sent, total_bytes)
    """
    path = Path(filepath)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {filepath}")
    data = path.read_bytes()
    task = _FileTask(path.name, data)
    _dispatch_tasks(peer_ip, peer_port, [task], sender_name,
                    len(data), progress_cb)
    logger.info(f"File sent: {path.name}  {fmt_size(len(data))}")


def send_folder(peer_ip: str, peer_port: int, folder_path: str,
                sender_name: str, progress_cb=None):
    """
    Send an entire folder without zipping.

    Every file is split into chunks, compressed, and ALL chunks from ALL
    files are dispatched in parallel (bounded by MAX_PARALLEL connections).

    The receiver recreates the full folder tree under received/<folder_name>/.
    progress_cb(bytes_sent, total_bytes)
    """
    folder = Path(folder_path)
    if not folder.is_dir():
        raise NotADirectoryError(f"Not a directory: {folder_path}")

    all_files = sorted(f for f in folder.rglob("*") if f.is_file())
    if not all_files:
        raise ValueError(f"Folder is empty: {folder_path}")

    # Build tasks – rel_path is relative to folder's PARENT so that the
    # folder name itself is preserved on the receiver side.
    tasks: list[_FileTask] = []
    for file in all_files:
        rel = "/".join(file.relative_to(folder.parent).parts)   # forward slashes
        data = file.read_bytes()
        tasks.append(_FileTask(rel, data))

    total_bytes = sum(len(t.data) for t in tasks)
    logger.info(
        f"Sending folder {folder.name!r}  "
        f"{len(tasks)} file(s)  {fmt_size(total_bytes)}"
    )

    _dispatch_tasks(peer_ip, peer_port, tasks, sender_name,
                    total_bytes, progress_cb)
    logger.info(f"Folder sent: {folder.name}")


def send_message(peer_ip: str, peer_port: int, text: str, sender_name: str):
    """Send a plain text message."""
    header = {"type": "message", "text": text, "sender": sender_name}
    with socket.create_connection((peer_ip, peer_port), timeout=CONNECT_TIMEOUT) as s:
        _send_frame(s, header)


# ──────────────────────────────────────────────────────────────────────────── #
#  TransferServer                                                              #
# ──────────────────────────────────────────────────────────────────────────── #

class TransferServer:
    """
    Listens on a TCP port and handles incoming connections.

    Dispatches by header "type":
      session_init  → registers a new _Session
      chunk         → delivers compressed data to the correct session
      message       → calls on_message immediately

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

        self._sessions: dict[str, _Session] = {}
        # Chunks that arrived before their session_init was registered.
        # keyed by session_id → list of (index, compressed, sha256)
        self._pending: dict[str, list[tuple[int, bytes, str]]] = {}
        self._sess_lock = threading.Lock()

        self._running = False
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(64)
        self._sock.settimeout(1.0)
        self._running = True
        self._thread = threading.Thread(
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

    # ── internal ────────────────────────────────────────────────────────── #

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
        rel_path = hdr.get("rel_path", hdr.get("filename", "unknown_file"))
        # Sanitise: forward slashes, no path traversal
        rel_path = rel_path.replace("\\", "/").lstrip("/").lstrip("../")

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
            # Replay any chunks that arrived before this session_init
            buffered = self._pending.pop(sid, [])
        logger.debug(
            f"Session registered {sid[:8]}…  {rel_path}"
            + (f"  (replaying {len(buffered)} buffered chunk(s))" if buffered else "")
        )
        # Process buffered chunks outside the lock
        for b_idx, b_comp, b_sha in buffered:
            try:
                session.add_chunk(b_idx, b_comp, b_sha)
            except Exception as exc:
                logger.error(f"Buffered chunk error {sid[:8]} idx={b_idx}: {exc}")

    def _h_chunk(self, conn: socket.socket, hdr: dict, sender_ip: str):
        sid  = hdr["session_id"]
        idx  = int(hdr["index"])
        size = int(hdr["compressed_size"])
        sha  = hdr["sha256"]

        compressed = _recv_exact(conn, size)

        with self._sess_lock:
            session = self._sessions.get(sid)
            if session is None:
                # session_init not yet received – buffer the chunk
                self._pending.setdefault(sid, []).append((idx, compressed, sha))
                logger.debug(f"Chunk #{idx} buffered (session {sid[:8]}… not yet registered)")
                return

        session.add_chunk(idx, compressed, sha)

    def _on_done(self, session_id: str, sender_name: str,
                 path: str, elapsed: float, speed: float, size: int):
        with self._sess_lock:
            self._sessions.pop(session_id, None)
        if self._on_file:
            self._on_file(sender_name, path, elapsed, speed, size)
