"""
main.py
───────
Interactive CLI entry point for the LDT (LAN Data Transfer) tool.

Usage
─────
  python main.py [--name <NodeName>] [--port <TCP port>]

Controls
─────────
  peers          – List active peers
  send           – Send a file or message to a peer
  help           – Show this help
  quit / exit    – Quit
"""

import argparse
import logging
import sys
import threading
import os

from peer_manager import PeerManager
from discovery import DiscoveryService
from transfer import TransferServer, send_file, send_message

# ────────────────────────────────── logging ──────────────────────────────── #

logging.basicConfig(
    level=logging.WARNING,               # suppress noise in interactive use
    format="%(levelname)s  %(name)s: %(message)s",
)

# ─────────────────────────────── print helpers ───────────────────────────── #

CYAN    = "\033[96m"
GREEN   = "\033[92m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
BOLD    = "\033[1m"
RESET   = "\033[0m"


def cprint(text, color="", bold=False):
    prefix = (BOLD if bold else "") + color
    print(f"{prefix}{text}{RESET}")


def _separator():
    print("─" * 55)


# ─────────────────────────────── callbacks ───────────────────────────────── #

def on_peer_joined(peer):
    print(f"\n{GREEN}[+] Peer joined:  {peer}{RESET}")
    _prompt()


def on_peer_left(peer):
    print(f"\n{YELLOW}[-] Peer left:    {peer}{RESET}")
    _prompt()


def on_message(sender, text):
    print(f"\n{CYAN}[MSG] {sender}: {text}{RESET}")
    _prompt()


def on_file(sender, filepath):
    print(f"\n{GREEN}[FILE] {sender} → saved to: {filepath}{RESET}")
    _prompt()


_input_lock = threading.Lock()


def _prompt():
    """Re-print the prompt after an async notification."""
    print("ldt> ", end="", flush=True)


# ───────────────────────────────── commands ──────────────────────────────── #

def cmd_peers(pm: PeerManager):
    peers = pm.get_peers()
    if not peers:
        cprint("  (no peers discovered yet – waiting for broadcasts…)", YELLOW)
        return
    _separator()
    for i, p in enumerate(peers, 1):
        cprint(f"  {i}. {p.name:<20} {p.ip}:{p.port}", CYAN)
    _separator()


def cmd_send(pm: PeerManager, node_name: str):
    peers = pm.get_peers()
    if not peers:
        cprint("  No peers available.", YELLOW)
        return

    # ── choose peer ──
    _separator()
    for i, p in enumerate(peers, 1):
        print(f"  {i}. {p.name} ({p.ip}:{p.port})")
    _separator()
    raw = input("Select peer number (or 'c' to cancel): ").strip()
    if raw.lower() == "c":
        return
    try:
        idx = int(raw) - 1
        peer = peers[idx]
    except (ValueError, IndexError):
        cprint("  Invalid selection.", RED)
        return

    # ── choose type ──
    kind = input("Send [f]ile or [m]essage? ").strip().lower()

    if kind in ("f", "file"):
        path = input("Enter file path: ").strip().strip('"').strip("'")
        if not os.path.isfile(path):
            cprint(f"  File not found: {path}", RED)
            return

        def progress(sent, total):
            pct = sent * 100 // total
            bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
            print(f"\r  [{bar}] {pct:3d}%  {sent}/{total} bytes", end="", flush=True)

        try:
            print(f"  Sending '{os.path.basename(path)}' to {peer.name}…")
            send_file(peer.ip, peer.port, path, node_name, progress_cb=progress)
            print()   # newline after progress bar
            cprint(f"  ✓ File sent successfully.", GREEN)
        except Exception as exc:
            print()
            cprint(f"  ✗ Error: {exc}", RED)

    elif kind in ("m", "message"):
        text = input("Enter message: ").strip()
        if not text:
            return
        try:
            send_message(peer.ip, peer.port, text, node_name)
            cprint(f"  ✓ Message sent.", GREEN)
        except Exception as exc:
            cprint(f"  ✗ Error: {exc}", RED)
    else:
        cprint("  Unknown type. Use 'f' or 'm'.", YELLOW)


def cmd_help():
    _separator()
    print(f"  {BOLD}Commands:{RESET}")
    print("  peers   – Show active peers")
    print("  send    – Send file or message to a peer")
    print("  help    – Show this help")
    print("  quit    – Exit")
    _separator()


# ─────────────────────────────────── main ───────────────────────────────── #

def main():
    parser = argparse.ArgumentParser(description="LDT – LAN Data Transfer")
    parser.add_argument("--name", default=None, help="Node name (default: hostname)")
    parser.add_argument("--port", type=int, default=9000, help="TCP port (default: 9000)")
    args = parser.parse_args()

    import socket as _socket
    node_name = args.name or _socket.gethostname()
    tcp_port  = args.port

    # ── Banner ──
    os.system("")  # enable ANSI on Windows
    print()
    cprint("  ██╗     ██████╗ ████████╗", CYAN, bold=True)
    cprint("  ██║     ██╔══██╗╚══██╔══╝", CYAN, bold=True)
    cprint("  ██║     ██║  ██║   ██║   ", CYAN, bold=True)
    cprint("  ██║     ██║  ██║   ██║   ", CYAN, bold=True)
    cprint("  ███████╗██████╔╝   ██║   ", CYAN, bold=True)
    cprint("  ╚══════╝╚═════╝    ╚═╝   ", CYAN, bold=True)
    cprint("  LAN Data Transfer", bold=True)
    print()
    cprint(f"  Node : {node_name}", BOLD)
    cprint(f"  Port : {tcp_port}", BOLD)
    cprint("  Type 'help' for commands", YELLOW)
    _separator()

    # ── Setup ──
    pm = PeerManager(on_peer_joined=on_peer_joined, on_peer_left=on_peer_left)
    pm.start()

    discovery = DiscoveryService(node_name, tcp_port, pm)
    try:
        discovery.start()
    except OSError as exc:
        cprint(f"Discovery failed to start: {exc}", RED)
        sys.exit(1)

    server = TransferServer(
        port=tcp_port,
        save_dir="received",
        on_message=on_message,
        on_file=on_file,
    )
    try:
        server.start()
    except OSError as exc:
        cprint(f"Transfer server failed to start: {exc}", RED)
        cprint(f"Hint: port {tcp_port} may already be in use. Try --port <other>", YELLOW)
        sys.exit(1)

    # ── REPL ──
    try:
        while True:
            try:
                cmd = input("ldt> ").strip().lower()
            except EOFError:
                break

            if cmd in ("quit", "exit", "q"):
                break
            elif cmd in ("peers", "p"):
                cmd_peers(pm)
            elif cmd in ("send", "s"):
                cmd_send(pm, node_name)
            elif cmd in ("help", "h", "?"):
                cmd_help()
            elif cmd == "":
                pass
            else:
                cprint(f"  Unknown command: '{cmd}'. Type 'help'.", YELLOW)

    except KeyboardInterrupt:
        pass
    finally:
        print("\nShutting down…")
        discovery.stop()
        server.stop()
        pm.stop()
        cprint("Goodbye!", GREEN)


if __name__ == "__main__":
    main()
