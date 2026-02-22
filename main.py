"""
main.py  v5
-----------
Interactive CLI for LDT – LAN Data Transfer.

Commands (interactive):
  peers         – List active peers
  send          – Interactive send wizard
  help          – Show help
  quit / exit   – Quit

Inline send syntax (no prompts):
  send <peer#> f <path>        – File
  send <peer#> d <folder>      – Directory
  send <peer#> m <text>        – Message

Command chaining (semicolon-separated):
  send 1 f "a.txt"; send 1 f "b.txt"; peers

Usage:
  python main.py [--name <name>] [--port <port>]
                 [--save-dir <dir>] [--debug]
"""

import argparse
import logging
import os
import shlex
import socket as _socket
import sys
import threading
import time
from pathlib import Path

from discovery import DiscoveryService
from peer_manager import PeerManager
from transfer import TransferServer, fmt_size, send_file, send_folder, send_message

# --------------------------- logging --------------------------------------- #

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s  %(name)s: %(message)s",
)

# --------------------------- ANSI colours ---------------------------------- #

os.system("")   # enable ANSI codes on Windows
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"


def _c(text, color="", bold=False) -> str:
    return (BOLD if bold else "") + color + str(text) + RESET


def _sep():
    print("-" * 60)


# ----------------------- thread-safe print helpers ------------------------- #

_print_lock = threading.Lock()


def _safe_print(*args, end="\n", flush=False):
    with _print_lock:
        print(*args, end=end, flush=flush)


def _prompt():
    with _print_lock:
        print("ldt> ", end="", flush=True)


# ---------------------------- Argument parser ------------------------------ #

def _parse_cmd(line: str) -> list[str]:
    r"""
    Shell-style tokeniser that handles Windows backslash paths correctly.

    Uses posix=False so backslashes are NOT treated as escape characters
    (critical for Windows paths like E:\History\file.iso).
    Surrounding quotes are stripped manually after splitting.
    """
    try:
        tokens = shlex.split(line, posix=False)
    except ValueError:
        return line.split()
    # Strip surrounding double or single quotes from each token
    result = []
    for t in tokens:
        if len(t) >= 2 and ((t[0] == '"' and t[-1] == '"')
                            or (t[0] == "'" and t[-1] == "'")):
            t = t[1:-1]
        result.append(t)
    return result


# --------------------------- Event callbacks ------------------------------- #

def on_peer_joined(peer):
    _safe_print(f"\n{GREEN}[+] Peer joined : {peer}{RESET}")
    _prompt()


def on_peer_left(peer):
    _safe_print(f"\n{YELLOW}[-] Peer left   : {peer}{RESET}")
    _prompt()


def on_message(sender, text):
    _safe_print(f"\n{CYAN}[MSG] {BOLD}{sender}{RESET}{CYAN}: {text}{RESET}")
    _prompt()


# Per-file receive-start timestamps for speedometer
_recv_sessions: dict[str, float] = {}


def on_receive_progress(received: int, total: int, sender: str, filename: str):
    """Called from receiver threads as each chunk arrives."""
    if total == 0:
        return
    key = f"{sender}/{filename}"
    if key not in _recv_sessions:
        _recv_sessions[key] = time.time()
    elapsed = max(time.time() - _recv_sessions[key], 0.001)
    speed = received / elapsed
    pct  = min(received * 100 // total, 100)
    done = pct >= 100
    bar  = "#" * (pct // 5) + "-" * (20 - pct // 5)
    line = (
        f"\r  {DIM}[RECV]{RESET} {_c(sender, CYAN, bold=True)} -> "
        f"{filename[:18]:<18}  [{bar}] {pct:3d}%  "
        f"{fmt_size(received)}/{fmt_size(total)}  "
        f"{_c(fmt_size(int(speed)) + '/s', DIM)}  "
    )
    with _print_lock:
        print(line, end="", flush=True)
        if done:
            print()
            _recv_sessions.pop(key, None)


def on_file(sender: str, path: str, elapsed: float, speed: float, size: int):
    """Called after a transfer is fully assembled and verified."""
    name = os.path.basename(path)
    with _print_lock:
        print()
        print(f"\n{GREEN}[FILE OK] {BOLD}{sender}{RESET}{GREEN} -> {name}{RESET}")
        print(f"   {DIM}Saved : {path}{RESET}")
        print(f"   {DIM}Size  : {fmt_size(size)}  in {elapsed:.1f}s  "
              f"@ {fmt_size(int(speed))}/s{RESET}")
    _prompt()


# ------------------------------ CLI commands ------------------------------- #

def cmd_peers(pm: PeerManager):
    peers = pm.get_peers()
    _sep()
    if not peers:
        print(f"  {YELLOW}No peers yet – waiting for broadcasts...{RESET}")
    else:
        for i, p in enumerate(peers, 1):
            tag   = f"{p.ip}:{p.port}"
            ago   = p.time_ago
            ago_s = f"{ago}s ago" if ago < 60 else f"{ago // 60}m ago"
            print(f"  {BOLD}{i}.{RESET}  {_c(p.name, CYAN, bold=True):<28}  "
                  f"{DIM}{tag}   last seen {ago_s}{RESET}")
    _sep()


def cmd_send(pm: PeerManager, node_name: str,
             inline_args: list[str] | None = None):
    """
    Send a file, folder, or message.

    Inline mode  – all args supplied on the command line (no prompts):
      send 1 f "C:\\path\\file.txt"
      send 2 d /my/folder
      send 1 m Hello world

    Interactive – any missing arg falls back to a prompt.
    """
    peers = pm.get_peers()
    if not peers:
        print(f"  {YELLOW}No peers available.{RESET}")
        return

    args = list(inline_args) if inline_args else []

    # -- 1. Peer ---------------------------------------------------------
    if args and args[0].isdigit():
        idx = int(args.pop(0)) - 1
        if not (0 <= idx < len(peers)):
            print(f"  {RED}Peer #{idx + 1} not found. Use 'peers' to list.{RESET}")
            return
        peer = peers[idx]
    else:
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

    # -- 2. Type ---------------------------------------------------------
    if args:
        kind = args.pop(0).lower()
    else:
        kind = input("Send [f]ile, [d]irectory, or [m]essage? ").strip().lower()

    # -- 3. Path / text --------------------------------------------------
    # Join remaining tokens to support unquoted paths with spaces
    inline_val = " ".join(args).strip().strip('"').strip("'") if args else None

    # --- file ------------------------------------------------------------
    if kind in ("f", "file"):
        path = inline_val or input("File path: ").strip().strip('"').strip("'")
        if not os.path.isfile(path):
            print(f"  {RED}File not found: {path}{RESET}")
            return

        size = os.path.getsize(path)
        print(f"  Sending {_c(os.path.basename(path), CYAN)}  ({fmt_size(size)})  "
              f"-> {_c(peer.name, BOLD)} ...")

        _t0 = time.time()

        def progress(sent, total, _t=_t0):
            elapsed = max(time.time() - _t, 0.001)
            speed   = sent / elapsed
            pct = min(sent * 100 // total, 100) if total else 0
            bar = "#" * (pct // 5) + "-" * (20 - pct // 5)
            print(
                f"\r  [{bar}] {pct:3d}%   "
                f"{fmt_size(sent)}/{fmt_size(total)}  "
                f"{_c(fmt_size(int(speed)) + '/s', DIM)}",
                end="", flush=True,
            )

        try:
            send_file(peer.ip, peer.port, path, node_name, progress_cb=progress)
            print(f"\n  {GREEN}OK Sent successfully.{RESET}")
        except Exception as exc:
            print(f"\n  {RED}ERR Error: {exc}{RESET}")

    # --- directory -------------------------------------------------------
    elif kind in ("d", "dir", "directory", "folder"):
        path = inline_val or input("Folder path: ").strip().strip('"').strip("'")
        if not os.path.isdir(path):
            print(f"  {RED}Directory not found: {path}{RESET}")
            return

        name          = os.path.basename(path.rstrip("/\\"))
        folder_p      = Path(path)
        file_count    = sum(1 for f in folder_p.rglob("*") if f.is_file())
        total_sz      = sum(f.stat().st_size for f in folder_p.rglob("*") if f.is_file())
        print(f"  Sending folder {_c(name, CYAN)}  "
              f"{_c(file_count, BOLD)} file(s)  {fmt_size(total_sz)}  "
              f"-> {_c(peer.name, BOLD)} ...")
        print(f"  {DIM}(chunked parallel streaming – no archive created){RESET}")

        _t0 = time.time()

        def progress(sent, total, _t=_t0):
            elapsed = max(time.time() - _t, 0.001)
            speed   = sent / elapsed
            pct = min(sent * 100 // total, 100) if total else 0
            bar = "#" * (pct // 5) + "-" * (20 - pct // 5)
            print(
                f"\r  [{bar}] {pct:3d}%   "
                f"{fmt_size(sent)}/{fmt_size(total)}  "
                f"{_c(fmt_size(int(speed)) + '/s', DIM)}",
                end="", flush=True,
            )

        try:
            send_folder(peer.ip, peer.port, path, node_name, progress_cb=progress)
            print(f"\n  {GREEN}OK Folder sent successfully.{RESET}")
        except Exception as exc:
            print(f"\n  {RED}ERR Error: {exc}{RESET}")

    # --- message ---------------------------------------------------------
    elif kind in ("m", "message", "msg"):
        text = inline_val or input("Message: ").strip()
        if not text:
            return
        try:
            send_message(peer.ip, peer.port, text, node_name)
            print(f"  {GREEN}OK Message sent.{RESET}")
        except Exception as exc:
            print(f"  {RED}ERR Error: {exc}{RESET}")

    else:
        print(f"  {YELLOW}Unknown type '{kind}'. Use 'f', 'd', or 'm'.{RESET}")


def cmd_help():
    _sep()
    print(f"  {BOLD}Basic commands:{RESET}")
    print(f"  {CYAN}peers{RESET}     – List active peers (with last-seen time)")
    print(f"  {CYAN}send{RESET}      – Interactive send wizard")
    print(f"  {CYAN}help{RESET}      – This help")
    print(f"  {CYAN}quit{RESET}      – Exit")
    _sep()
    print(f"  {BOLD}Inline send:{RESET}  {DIM}send <peer#> <type> <path/text>{RESET}")
    print(f"  {CYAN}send 1 f{RESET} {DIM}\"C:\\path\\file.txt\"{RESET}   – send file to peer 1")
    print(f"  {CYAN}send 2 d{RESET} {DIM}/my/folder{RESET}             – send folder to peer 2")
    print(f"  {CYAN}send 1 m{RESET} {DIM}Hello world{RESET}            – send message to peer 1")
    _sep()
    print(f"  {BOLD}Command chaining{RESET}  {DIM}(separate with ;){RESET}")
    print(f'  {DIM}send 1 f "a.txt"; send 1 f "b.txt"; peers{RESET}')
    _sep()
    print(f"  {BOLD}Transfer engine:{RESET}")
    print(f"  {DIM}* zlib compression (level 6) per chunk{RESET}")
    print(f"  {DIM}* 4 parallel chunk streams with retry (3×){RESET}")
    print(f"  {DIM}* SHA-256 integrity: per-chunk + full-file{RESET}")
    print(f"  {DIM}* Folder: per-file streaming (no zip){RESET}")
    print(f"  {DIM}* Received files saved to: <save-dir>/{RESET}")
    _sep()


# -------------------- command dispatcher ----------------------------------- #

def _run_cmd(cmd_str: str, pm: PeerManager, node_name: str) -> bool:
    """
    Parse and execute a single command string.
    Returns True if the application should quit.
    """
    tokens = _parse_cmd(cmd_str)
    if not tokens:
        return False

    cmd        = tokens[0].lower()
    extra_args = tokens[1:]

    if cmd in ("quit", "exit", "q"):
        return True
    elif cmd in ("peers", "p"):
        cmd_peers(pm)
    elif cmd in ("send", "s"):
        cmd_send(pm, node_name, extra_args if extra_args else None)
    elif cmd in ("help", "h", "?"):
        cmd_help()
    else:
        print(f"  {YELLOW}Unknown command '{cmd}'. Type 'help'.{RESET}")

    return False


# ----------------------------------- main ---------------------------------- #

def main():
    parser = argparse.ArgumentParser(description="LDT – LAN Data Transfer v5")
    parser.add_argument("--name",     default=None,       help="Node name (default: hostname)")
    parser.add_argument("--port",     type=int, default=9000, help="TCP port (default: 9000)")
    parser.add_argument("--save-dir", default="received", help="Save directory (default: received)")
    parser.add_argument("--debug",    action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    node_name = args.name or _socket.gethostname()
    tcp_port  = args.port
    save_dir  = args.save_dir

    # -- Banner --
    print()
    print(_c("  ##╗     ######╗ ########╗", CYAN, bold=True))
    print(_c("  ##║     ##╔══##╗╚══##╔══╝", CYAN, bold=True))
    print(_c("  ##║     ##║  ##║   ##║   ", CYAN, bold=True))
    print(_c("  ##║     ##║  ##║   ##║   ", CYAN, bold=True))
    print(_c("  #######╗######╔╝   ##║   ", CYAN, bold=True))
    print(_c("  ╚══════╝╚═════╝    ╚═╝   ", CYAN, bold=True))
    print(_c("  LAN Data Transfer  v5", bold=True))
    print()
    print(f"  {BOLD}Node     :{RESET}  {_c(node_name, CYAN, bold=True)}")
    print(f"  {BOLD}Port     :{RESET}  {tcp_port}")
    print(f"  {BOLD}Save dir :{RESET}  {save_dir}")
    print(f"  {DIM}Compression: zlib.6   Parallel: 4   Integrity: SHA-256{RESET}")
    print(f"  {BOLD}--debug  :{RESET}  {'on' if args.debug else 'off'}  "
          f"  {YELLOW}type 'help' for commands{RESET}")
    _sep()

    # -- Services --
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
        save_dir=save_dir,
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

    # -- REPL --
    try:
        while True:
            try:
                raw_line = input("ldt> ").strip()
            except EOFError:
                break

            if not raw_line:
                continue

            # Semicolon chaining: "send 1 f a.txt; send 1 f b.txt; peers"
            cmd_parts = [c.strip() for c in raw_line.split(";") if c.strip()]
            should_quit = False

            for cmd_str in cmd_parts:
                if _run_cmd(cmd_str, pm, node_name):
                    should_quit = True
                    break

            if should_quit:
                break

    except KeyboardInterrupt:
        pass
    finally:
        print(f"\n{DIM}Shutting down...{RESET}")
        discovery.stop()
        server.stop()
        pm.stop()
        print(f"{GREEN}Goodbye!{RESET}")


if __name__ == "__main__":
    main()
