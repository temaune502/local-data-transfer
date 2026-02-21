"""
discovery.py
────────────
UDP-based peer discovery using broadcast packets.

Each node continuously:
  1. Broadcasts an ANNOUNCE packet every ANNOUNCE_INTERVAL seconds.
  2. Listens for ANNOUNCE packets from other nodes and adds them to the
     PeerManager registry.

No central server is required. All nodes are equal.
"""

import socket
import json
import threading
import time
import logging

logger = logging.getLogger(__name__)

BROADCAST_PORT = 50000          # Well-known port for discovery
ANNOUNCE_INTERVAL = 5           # Seconds between broadcasts
BUFFER_SIZE = 1024


class DiscoveryService:
    """
    Announces this node on the LAN and discovers other nodes.

    Parameters
    ----------
    node_name  : Human-readable name for this PC/node.
    tcp_port   : Port where this node's TCP TransferServer is listening.
    peer_manager: PeerManager instance to register discovered peers.
    """

    def __init__(self, node_name: str, tcp_port: int, peer_manager):
        self.node_name = node_name
        self.tcp_port = tcp_port
        self.peer_manager = peer_manager

        self._running = False
        self._sock: socket.socket | None = None

        self._announce_thread: threading.Thread | None = None
        self._listen_thread: threading.Thread | None = None

    # ------------------------------------------------------------------ #
    #  Public                                                              #
    # ------------------------------------------------------------------ #

    def start(self):
        """Open the UDP socket and launch background threads."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.settimeout(1.0)  # so the listener loop can check _running

        try:
            self._sock.bind(("", BROADCAST_PORT))
        except OSError as exc:
            logger.error(f"Cannot bind UDP discovery socket: {exc}")
            raise

        self._running = True

        self._listen_thread = threading.Thread(
            target=self._listen_loop, name="DiscoveryListener", daemon=True
        )
        self._announce_thread = threading.Thread(
            target=self._announce_loop, name="DiscoveryAnnouncer", daemon=True
        )

        self._listen_thread.start()
        self._announce_thread.start()
        logger.info(
            f"Discovery started  name={self.node_name!r}  tcp_port={self.tcp_port}"
        )

    def stop(self):
        """Signal threads to stop and close the socket."""
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        logger.info("Discovery stopped.")

    # ------------------------------------------------------------------ #
    #  Internal                                                            #
    # ------------------------------------------------------------------ #

    def _build_packet(self) -> bytes:
        payload = {
            "name": self.node_name,
            "port": self.tcp_port,
        }
        return json.dumps(payload).encode("utf-8")

    def _announce_loop(self):
        """Broadcast ANNOUNCE packets at regular intervals."""
        packet = self._build_packet()
        while self._running:
            try:
                self._sock.sendto(packet, ("<broadcast>", BROADCAST_PORT))
                logger.debug(f"Announced: {packet}")
            except OSError:
                if self._running:
                    logger.warning("Failed to send broadcast packet.")
            time.sleep(ANNOUNCE_INTERVAL)

    def _listen_loop(self):
        """Listen for ANNOUNCE packets from other nodes."""
        while self._running:
            try:
                data, addr = self._sock.recvfrom(BUFFER_SIZE)
                sender_ip = addr[0]
                self._handle_packet(sender_ip, data)
            except socket.timeout:
                continue
            except OSError:
                if self._running:
                    logger.warning("UDP recv error.")
                break

    def _handle_packet(self, sender_ip: str, data: bytes):
        """Parse a received broadcast packet and update the peer registry."""
        try:
            payload = json.loads(data.decode("utf-8"))
            name = payload.get("name", sender_ip)
            port = int(payload.get("port", 9000))

            # Ignore our own packets by comparing against all local IPs
            if sender_ip in _local_ips():
                return

            self.peer_manager.add_or_refresh(sender_ip, port, name)
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            logger.debug(f"Bad discovery packet from {sender_ip}: {exc}")


def _local_ips() -> set[str]:
    """Return all IP addresses assigned to this machine."""
    ips: set[str] = {"127.0.0.1"}
    try:
        hostname = socket.gethostname()
        for info in socket.getaddrinfo(hostname, None):
            ips.add(info[4][0])
    except Exception:
        pass
    return ips
