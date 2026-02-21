"""
main.py  v2
───────────
Interactive CLI for LDT – LAN Data Transfer.

Commands
─────────
  peers          – List active peers
  send           – Send a file, folder, or message
  help           – Show help
  quit / exit    – Quit

Usage
─────
  python main.py [--name <name>] [--port <port>]
"""

import argparse
import logging
import os
import socket as _socket
import sys
import threading

from discovery import DiscoveryService
from peer_manager import PeerManager
from transfer import TransferServer, fmt_size, send_file, send_folder, send_message

# ─────────────────────────── logging ─────────────────────────────────────── #

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s  %(name)s: %(message)s",
)

# ─────────────────────────── ANSI colours ────────────────────────────────── #

os.system("")   # enable ANSI codes on Windows
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
MAGENTA= "\033[95m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"


def _c(text, color="", bold=False) -> str:
    return (BOLD if bold else "") + color + str(text) + RESET


def _sep():
    print("─" * 58)


# ─────────────────────── thread-safe print helpers ───────────────────────── #

_print_lock = threading.Lock()


def _safe_print(*args, end="\n", flush=False):
    with _print_lock:
        print(*args, end=end, flush=flush)


def _prompt():
    with _print_lock:
        print("ldt> ", end="", flush=True)


# ─────────────────────────── Event callbacks ─────────────────────────────── #

def on_peer_joined(peer):
    _safe_print(f"\n{GREEN}[+] Peer joined : {peer}{RESET}")
    _prompt()


def on_peer_left(peer):
    _safe_print(f"\n{YELLOW}[-] Peer left   : {peer}{RESET}")
    _prompt()


def on_message(sender, text):
    _safe_print(f"\n{CYAN}[MSG] {BOLD}{sender}{RESET}{CYAN}: {text}{RESET}")
    _prompt()


# Tracks whether a receive-progress line is active (needs clearing)
_recv_progress_active = threading.local()


def on_receive_progress(received: int, total: int, sender: str, filename: str):
    """Called from receiver threads as each chunk arrives."""
    if total == 0:
        return
    pct = min(received * 100 // total, 100)
    done = pct >= 100
    bar  = "█" * (pct // 5) + "░" * (20 - pct // 5)
    line = (
        f"\r  {DIM}[RECV]{RESET} {_c(sender, CYAN, bold=True)} → "
        f"{filename[:22]:<22}  [{bar}] {pct:3d}%  "
        f"{fmt_size(received)}/{fmt_size(total)}"
    )
    with _print_lock:
        print(line, end="", flush=True)
        if done:
            print()   # newline when complete


def on_file(sender: str, path: str, elapsed: float, speed: float, size: int):
    """Called after a transfer is fully assembled and verified."""
    name = os.path.basename(path)
    with _print_lock:
        print()  # ensure fresh line after any progress output
        print(
            f"\n{GREEN}[FILE ✓] {BOLD}{sender}{RESET}{GREEN} → {name}{RESET}"
        )
        print(
            f"   {DIM}Saved : {path}{RESET}"
        )
        print(
            f"   {DIM}Size  : {fmt_size(size)}  in {elapsed:.1f}s  "
            f"@ {fmt_size(int(speed))}/s{RESET}"
        )
    _prompt()


# ────────────────────────────── CLI commands ─────────────────────────────── #

def cmd_peers(pm: PeerManager):
    peers = pm.get_peers()
    _sep()
    if not peers:
        print(f"  {YELLOW}No peers yet – waiting for broadcasts…{RESET}")
    else:
        for i, p in enumerate(peers, 1):
            tag = f"{p.ip}:{p.port}"
            print(f"  {BOLD}{i}.{RESET}  {_c(p.name, CYAN, bold=True):<28}  {DIM}{tag}{RESET}")
    _sep()


def cmd_send(pm: PeerManager, node_name: str):
    peers = pm.get_peers()
    if not peers:
        print(f"  {YELLOW}No peers available.{RESET}")
        return

    # ── choose peer ──
    _sep()
    for i, p in enumerate(peers, 1):
        print(f"  {i}.  {_c(p.name, CYAN)}  {DIM}({p.ip}:{p.port}){RESET}")
    _sep()
    raw = input("Select peer (number) or [c]ancel: ").strip()
    if raw.lower() == "c":
        return
    try:
        peer = peers[int(raw) - 1]
    except (ValueError, IndexError):
        print(f"  {RED}Invalid selection.{RESET}")
        return

    # ── choose type ──
    kind = input("Send [f]ile, [d]irectory, or [m]essage? ").strip().lower()

    # ─────── file ───────
    if kind in ("f", "file"):
        path = input("File path: ").strip().strip('"').strip("'")
        if not os.path.isfile(path):
            print(f"  {RED}File not found: {path}{RESET}")
            return

        size = os.path.getsize(path)
        print(f"  Sending {_c(os.path.basename(path), CYAN)}  ({fmt_size(size)})  "
              f"→ {_c(peer.name, BOLD)} …")

        def progress(sent, total):
            pct = min(sent * 100 // total, 100) if total else 0
            bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
            print(
                f"\r  [{bar}] {pct:3d}%   {fmt_size(sent)}/{fmt_size(total)}",
                end="", flush=True,
            )

        try:
            send_file(peer.ip, peer.port, path, node_name, progress_cb=progress)
            print(f"\n  {GREEN}✓ Sent successfully.{RESET}")
        except Exception as exc:
            print(f"\n  {RED}✗ Error: {exc}{RESET}")

    # ─────── directory ───────
    elif kind in ("d", "dir", "directory", "folder"):
        path = input("Folder path: ").strip().strip('"').strip("'")
        if not os.path.isdir(path):
            print(f"  {RED}Directory not found: {path}{RESET}")
            return

        name = os.path.basename(path.rstrip("/\\"))
        print(f"  Zipping and sending folder {_c(name, CYAN)} → {_c(peer.name, BOLD)} …")

        def progress(sent, total):
            pct = min(sent * 100 // total, 100) if total else 0
            bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
            print(
                f"\r  [{bar}] {pct:3d}%   {fmt_size(sent)}/{fmt_size(total)}",
                end="", flush=True,
            )

        try:
            send_folder(peer.ip, peer.port, path, node_name, progress_cb=progress)
            print(f"\n  {GREEN}✓ Folder sent successfully.{RESET}")
        except Exception as exc:
            print(f"\n  {RED}✗ Error: {exc}{RESET}")

    # ─────── message ───────
    elif kind in ("m", "message", "msg"):
        text = input("Message: ").strip()
        if not text:
            return
        try:
            send_message(peer.ip, peer.port, text, node_name)
            print(f"  {GREEN}✓ Message sent.{RESET}")
        except Exception as exc:
            print(f"  {RED}✗ Error: {exc}{RESET}")
    else:
        print(f"  {YELLOW}Use 'f', 'd', or 'm'.{RESET}")


def cmd_help():
    _sep()
    print(f"  {BOLD}Commands:{RESET}")
    print(f"  {CYAN}peers{RESET}   – Show active peers")
    print(f"  {CYAN}send{RESET}    – Send file / folder / message")
    print(f"  {CYAN}help{RESET}    – This help")
    print(f"  {CYAN}quit{RESET}    – Exit")
    _sep()
    print(f"  {BOLD}Transfer features:{RESET}")
    print(f"  {DIM}• zlib compression (level 6) per chunk{RESET}")
    print(f"  {DIM}• 4 parallel chunk streams{RESET}")
    print(f"  {DIM}• SHA-256 integrity: per-chunk + full-file{RESET}")
    print(f"  {DIM}• Folder support (auto-zip / auto-extract){RESET}")
    print(f"  {DIM}• Received files saved to: received/{RESET}")
    _sep()


# ─────────────────────────────────── main ────────────────────────────────── #

def main():
    parser = argparse.ArgumentParser(description="LDT – LAN Data Transfer v2")
    parser.add_argument("--name", default=None, help="Node name (default: hostname)")
    parser.add_argument("--port", type=int, default=9000, help="TCP port (default: 9000)")
    args = parser.parse_args()

    node_name = args.name or _socket.gethostname()
    tcp_port  = args.port

    # ── Banner ──
    print()
    print(_c("  ██╗     ██████╗ ████████╗", CYAN, bold=True))
    print(_c("  ██║     ██╔══██╗╚══██╔══╝", CYAN, bold=True))
    print(_c("  ██║     ██║  ██║   ██║   ", CYAN, bold=True))
    print(_c("  ██║     ██║  ██║   ██║   ", CYAN, bold=True))
    print(_c("  ███████╗██████╔╝   ██║   ", CYAN, bold=True))
    print(_c("  ╚══════╝╚═════╝    ╚═╝   ", CYAN, bold=True))
    print(_c("  LAN Data Transfer  v2", bold=True))
    print()
    print(f"  {BOLD}Node :{RESET}  {_c(node_name, CYAN, bold=True)}")
    print(f"  {BOLD}Port :{RESET}  {tcp_port}")
    print(f"  {DIM}Compression: zlib·6   Parallel: 4   Integrity: SHA-256{RESET}")
    print(f"  {YELLOW}Type 'help' for commands{RESET}")
    _sep()

    # ── Services ──
    pm = PeerManager(on_peer_joined=on_peer_joined, on_peer_left=on_peer_left)
    pm.start()

    discovery = DiscoveryService(node_name, tcp_port, pm)
    try:
        discovery.start()
    except OSError as exc:
        print(f"{RED}Discovery error: {exc}{RESET}")
        sys.exit(1)

    server = TransferServer(
        port=tcp_port,
        save_dir="received",
        on_message=on_message,
        on_file=on_file,
        on_receive_progress=on_receive_progress,
    )
    try:
        server.start()
    except OSError as exc:
        print(f"{RED}TransferServer error: {exc}{RESET}")
        print(f"{YELLOW}Hint: port {tcp_port} may be in use. Try --port <other>{RESET}")
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
                print(f"  {YELLOW}Unknown command '{cmd}'. Type 'help'.{RESET}")

    except KeyboardInterrupt:
        pass
    finally:
        print(f"\n{DIM}Shutting down…{RESET}")
        discovery.stop()
        server.stop()
        pm.stop()
        print(f"{GREEN}Goodbye!{RESET}")


if __name__ == "__main__":
    main()
