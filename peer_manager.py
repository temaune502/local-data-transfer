"""
peer_manager.py
───────────────
Thread-safe registry of known peers in the local network.
Each peer entry has a TTL; entries that are not refreshed within
the timeout window are automatically removed by a background thread.
"""

import threading
import time
import logging

logger = logging.getLogger(__name__)

# Peer is considered dead after this many seconds without a heartbeat
PEER_TTL_SECONDS = 15


class PeerInfo:
    """Holds metadata about one remote peer."""

    def __init__(self, ip: str, port: int, name: str):
        self.ip = ip
        self.port = port
        self.name = name
        self.last_seen = time.time()

    def refresh(self):
        self.last_seen = time.time()

    def is_alive(self) -> bool:
        return (time.time() - self.last_seen) < PEER_TTL_SECONDS

    def __str__(self):
        return f"{self.name} ({self.ip}:{self.port})"


class PeerManager:
    """
    Manages the list of active peers.

    Thread-safe: all public methods acquire the internal lock before
    reading or modifying the peer table.
    """

    def __init__(self, on_peer_joined=None, on_peer_left=None):
        """
        Parameters
        ----------
        on_peer_joined : callable(PeerInfo) | None
            Called whenever a *new* peer is first seen.
        on_peer_left   : callable(PeerInfo) | None
            Called whenever a peer is removed (TTL expired or explicit).
        """
        self._peers: dict[str, PeerInfo] = {}  # keyed by IP
        self._lock = threading.Lock()
        self._on_joined = on_peer_joined
        self._on_left = on_peer_left

        self._running = False
        self._cleanup_thread: threading.Thread | None = None

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def start(self):
        """Start the background cleanup thread."""
        self._running = True
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop, name="PeerCleanup", daemon=True
        )
        self._cleanup_thread.start()
        logger.debug("PeerManager started.")

    def stop(self):
        """Stop the background cleanup thread."""
        self._running = False

    def add_or_refresh(self, ip: str, port: int, name: str):
        """
        Register a peer or refresh its TTL if already known.
        Triggers on_peer_joined callback for genuinely new peers.
        """
        with self._lock:
            if ip in self._peers:
                self._peers[ip].refresh()
                # Update name/port in case they changed
                self._peers[ip].name = name
                self._peers[ip].port = port
            else:
                peer = PeerInfo(ip, port, name)
                self._peers[ip] = peer
                logger.info(f"New peer discovered: {peer}")
                if self._on_joined:
                    threading.Thread(
                        target=self._on_joined, args=(peer,), daemon=True
                    ).start()

    def remove(self, ip: str):
        """Explicitly remove a peer by IP."""
        with self._lock:
            peer = self._peers.pop(ip, None)
            if peer and self._on_left:
                threading.Thread(
                    target=self._on_left, args=(peer,), daemon=True
                ).start()

    def get_peers(self) -> list[PeerInfo]:
        """Return a snapshot list of all currently alive peers."""
        with self._lock:
            return [p for p in self._peers.values() if p.is_alive()]

    def get_peer(self, ip: str) -> PeerInfo | None:
        """Return a single peer by IP, or None if not found."""
        with self._lock:
            return self._peers.get(ip)

    def count(self) -> int:
        with self._lock:
            return len(self._peers)

    # ------------------------------------------------------------------ #
    #  Internal                                                            #
    # ------------------------------------------------------------------ #

    def _cleanup_loop(self):
        """Runs every 5 seconds; removes peers whose TTL has expired."""
        while self._running:
            time.sleep(5)
            self._evict_stale()

    def _evict_stale(self):
        with self._lock:
            stale = [ip for ip, p in self._peers.items() if not p.is_alive()]
        for ip in stale:
            with self._lock:
                peer = self._peers.pop(ip, None)
            if peer:
                logger.info(f"Peer lost (TTL expired): {peer}")
                if self._on_left:
                    threading.Thread(
                        target=self._on_left, args=(peer,), daemon=True
                    ).start()
