#!/usr/bin/env python3
"""Per-conversation prompt queue for Herdr agent panes.

A queue holds prompts to send later in a conversation. Queues are keyed by the
agent's native session reference (the same binding Herdr persists and resumes
after a restart), falling back to the pane ID when no session is reported.

State lives in one JSON file under the plugin state directory. Every write is
atomic and durable (temporary file, fsync, rename, directory fsync), and the
previous version is kept as a backup, so the queue survives a Herdr, WSL, or
host crash.

Inserting an entry pastes it into the agent's prompt without submitting it and
marks it pending. The `pane.agent_status_changed` event hook removes pending
entries when that pane's agent starts working, which is the submit signal.
Removed entries go to a bounded per-queue history so they can be restored.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import datetime as dt
import errno
import fcntl
import json
import os
import pathlib
import re
import socket
import subprocess
import sys
import time
import unicodedata
import uuid
from dataclasses import dataclass
from typing import Any, Iterator

PLUGIN_ID = "joeguice.prompt-queue"
PANE_ENTRYPOINT = "queue"
TOKEN_SOURCE = "plugin:joeguice.prompt-queue"
TOKEN_NAME = "queue"
SCHEMA_VERSION = 1
HISTORY_LIMIT = 100
DELETED_QUEUE_LIMIT = 20
MAX_ENTRY_BYTES = 64 * 1024
# A working transition this soon after an insert is treated as unrelated noise
# rather than the user submitting the inserted text.
SUBMIT_GRACE_SECONDS = 0.75
SOCKET_TIMEOUT_SECONDS = 10.0


class QueueError(RuntimeError):
    pass


class HerdrError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


# --------------------------------------------------------------------------
# Paths and durable storage


def default_state_dir() -> pathlib.Path:
    configured = os.environ.get("HERDR_PLUGIN_STATE_DIR")
    if configured:
        return pathlib.Path(configured)
    base = os.environ.get("XDG_STATE_HOME") or str(pathlib.Path.home() / ".local" / "state")
    return pathlib.Path(base) / "herdr" / "plugins" / PLUGIN_ID


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def fsync_directory(path: pathlib.Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write(path: pathlib.Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.next-{os.getpid()}")
    with open(temporary, "wb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    fsync_directory(path.parent)


def empty_data() -> dict[str, Any]:
    return {"version": SCHEMA_VERSION, "queues": {}, "deleted_queues": []}


def validate_data(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise QueueError("queue file is not a JSON object")
    version = value.get("version")
    if version != SCHEMA_VERSION:
        raise QueueError(f"unsupported queue file version {version!r}")
    queues = value.get("queues")
    if not isinstance(queues, dict):
        raise QueueError("queue file has no queues object")
    for key, queue in queues.items():
        if not isinstance(queue, dict) or not isinstance(queue.get("entries"), list):
            raise QueueError(f"queue {key!r} is malformed")
        queue.setdefault("history", [])
    value.setdefault("deleted_queues", [])
    return value


class Store:
    """Locked, crash-durable JSON store for every conversation queue."""

    def __init__(self, directory: pathlib.Path):
        self.directory = directory
        self.path = directory / "queues.json"
        self.backup_path = directory / "queues.json.bak"
        self.lock_path = directory / ".lock"
        self.notices: list[str] = []

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        self.directory.mkdir(parents=True, exist_ok=True)
        with open(self.lock_path, "a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def read(self) -> dict[str, Any]:
        with self._locked():
            return self._load()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[dict[str, Any]]:
        """Yield the current data under an exclusive lock; persist it if changed."""
        with self._locked():
            data = self._load()
            before = json.dumps(data, sort_keys=True)
            yield data
            if json.dumps(data, sort_keys=True) != before:
                self._save(data)

    def _read_file(self, path: pathlib.Path) -> dict[str, Any] | None:
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return None
        return validate_data(json.loads(raw))

    def _quarantine(self, path: pathlib.Path) -> pathlib.Path:
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        target = path.with_name(f"{path.name}.corrupt-{stamp}-{os.getpid()}")
        os.replace(path, target)
        fsync_directory(path.parent)
        return target

    def _load(self) -> dict[str, Any]:
        try:
            data = self._read_file(self.path)
        except (OSError, ValueError, QueueError) as main_error:
            if isinstance(main_error, QueueError) and "unsupported queue file version" in str(main_error):
                # Never overwrite a file written by a newer schema.
                raise
            try:
                backup = self._read_file(self.backup_path)
            except (OSError, ValueError, QueueError):
                backup = None
            quarantined = self._quarantine(self.path)
            if backup is not None:
                atomic_write(self.path, encode_data(backup))
                self.notices.append(
                    f"Recovered queues from backup; damaged file kept at {quarantined}"
                )
                return backup
            self.notices.append(f"Queue file was unreadable; kept at {quarantined}")
            return empty_data()
        return data if data is not None else empty_data()

    def _save(self, data: dict[str, Any]) -> None:
        try:
            previous = self.path.read_bytes()
        except FileNotFoundError:
            previous = None
        if previous is not None:
            atomic_write(self.backup_path, previous)
        atomic_write(self.path, encode_data(data))


def encode_data(data: dict[str, Any]) -> bytes:
    return (json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode()


# --------------------------------------------------------------------------
# Herdr socket client


class Herdr:
    def __init__(self, socket_path: str | None = None, *, proc_root=None, codex_locks=None):
        self.socket_path = socket_path or os.environ.get("HERDR_SOCKET_PATH")
        self.proc_root = pathlib.Path(proc_root) if proc_root is not None else pathlib.Path("/proc")
        self.codex_locks = (pathlib.Path(codex_locks) if codex_locks is not None
                            else pathlib.Path.home() / ".codex/thread-writer-locks").resolve()

    def request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if not self.socket_path:
            raise HerdrError("no_socket", "HERDR_SOCKET_PATH is not set")
        payload = json.dumps({"id": f"prompt-queue:{uuid.uuid4().hex[:8]}", "method": method, "params": params})
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(SOCKET_TIMEOUT_SECONDS)
            try:
                connection.connect(self.socket_path)
            except OSError as exc:
                raise HerdrError("socket_unavailable", str(exc)) from exc
            connection.sendall(payload.encode() + b"\n")
            buffer = b""
            while not buffer.endswith(b"\n"):
                chunk = connection.recv(65536)
                if not chunk:
                    break
                buffer += chunk
        try:
            response = json.loads(buffer)
        except json.JSONDecodeError as exc:
            raise HerdrError("bad_response", f"{method} returned invalid JSON") from exc
        error = response.get("error")
        if error:
            raise HerdrError(str(error.get("code", "error")), str(error.get("message", "")))
        result = response.get("result")
        return result if isinstance(result, dict) else {}

    def pane(self, pane_id: str) -> dict[str, Any]:
        pane = self.request("pane.get", {"pane_id": pane_id}).get("pane")
        if not isinstance(pane, dict):
            raise HerdrError("pane_not_found", pane_id)
        return pane

    def panes(self) -> list[dict[str, Any]]:
        panes = self.request("pane.list", {}).get("panes")
        return [pane for pane in panes or [] if isinstance(pane, dict)]

    def selected_codex_session(self, pane: dict[str, Any]) -> str | None:
        """Resolve the title's selected UUID only from this pane's live process."""
        prefix = codex_title_prefix(pane)
        if not prefix:
            return None
        try:
            info = self.request("pane.process_info", {"pane_id": pane["pane_id"]})["process_info"]
            if not isinstance(info, dict) or info.get("pane_id") != pane["pane_id"]:
                return None
            if not isinstance(info.get("foreground_processes"), list):
                return None
            processes = [p for p in info["foreground_processes"]
                         if isinstance(p, dict) and p.get("name") == "codex"]
            if len(processes) != 1:
                return None
            process = processes[0]
            pid = process["pid"]
            if type(pid) is not int or pid <= 0:
                return None
            root = self.proc_root / str(pid)

            def identity():
                return (root.stat().st_uid, (root / "exe").resolve().name,
                        str((root / "cwd").resolve(strict=True)),
                        (root / "stat").read_text().rsplit(") ", 1)[1].split()[19])

            before = identity()
            if (before[0] != os.getuid() or before[1] != "codex" or before[2] != process.get("cwd")
                    or before[2] not in (pane.get("cwd"), pane.get("foreground_cwd"))):
                return None
            sessions = set()
            for fd in (root / "fd").iterdir():
                try:
                    target = pathlib.Path(os.readlink(fd))
                except FileNotFoundError:
                    continue
                if target.parent == self.codex_locks and re.fullmatch(
                        r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\.lock", target.name):
                    sessions.add(target.stem)
            selected = [session for session in sessions if session.startswith(prefix)]
            return selected[0] if len(selected) == 1 and identity() == before else None
        except (OSError, ValueError, KeyError, TypeError, IndexError, HerdrError):
            return None

    @staticmethod
    def checked_context(expected: Conversation) -> dict[str, Any]:
        if not expected.terminal_id or not expected.session:
            raise QueueError("Live terminal and native conversation identity are required for delivery")
        return {"expected_terminal_id": expected.terminal_id,
                "expected_agent_session": copy.deepcopy(expected.session),
                "expected_terminal_title": expected.terminal_title}

    def paste(self, pane_id: str, text: str, *, expected: Conversation) -> None:
        # Checked input keeps bracketed-paste behavior without submission. The
        # server validates terminal, binding and raw title in the same dispatch.
        self.request("pane.send_input_checked",
                     {"pane_id": pane_id, "text": text, **self.checked_context(expected)})

    def submit(self, pane_id: str, text: str, is_agent: bool, *, expected: Conversation) -> None:
        if not is_agent:
            raise QueueError("An active native agent conversation is required for delivery")
        self.request("agent.prompt_checked",
                     {"target": pane_id, "text": text, **self.checked_context(expected)})

    def notify(self, title: str, body: str) -> None:
        self.request("notification.show", {"title": title, "body": body, "sound": "request"})

    def report_count(self, pane_id: str, count: int) -> None:
        value = f"{count} queued" if count else None
        self.request(
            "pane.report_metadata",
            {"pane_id": pane_id, "source": TOKEN_SOURCE, "tokens": {TOKEN_NAME: value}},
        )


# --------------------------------------------------------------------------
# Conversations and pure queue operations


@dataclass
class Conversation:
    key: str
    label: str
    pane_id: str
    agent: str | None
    cwd: str | None
    session: dict[str, Any] | None
    terminal_id: str | None = None
    terminal_title: str | None = None

    @property
    def pane_key(self) -> str:
        return pane_key(self.pane_id)


def pane_key(pane_id: str) -> str:
    return f"pane:{pane_id}"


def codex_title_prefix(pane: dict[str, Any]) -> str:
    # Managed title order: optional spinner, session-id, project. Codex 0.165.0
    # can shorten the UUID's final group to five characters followed by "...".
    title = pane.get("terminal_title_stripped") or pane.get("terminal_title") or ""
    if not isinstance(title, str):
        return ""
    match = re.match(
        r"^(?:[\u2800-\u28ff](?: \| |\s+))?"
        r"([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{5,12})(\.\.\.)?(?: \| |$)",
        title.strip(),
    )
    return match[1] if match and ((len(match[1]) == 36 and not match[2])
                                 or (len(match[1]) == 29 and match[2])) else ""


def codex_binding_matches_title(pane: dict[str, Any]) -> bool:
    session = pane.get("agent_session")
    value = session.get("value") if isinstance(session, dict) else None
    prefix = codex_title_prefix(pane)
    return bool(prefix and isinstance(value, str) and value.startswith(prefix))


def conversation_for_pane(pane: dict[str, Any], *, codex_session: str | None = None) -> Conversation:
    pane_id = str(pane.get("pane_id") or "")
    if not pane_id:
        raise QueueError("pane has no pane_id")
    agent = pane.get("agent") if isinstance(pane.get("agent"), str) else None
    cwd = pane.get("foreground_cwd") or pane.get("cwd")
    session = pane.get("agent_session")
    if (isinstance(session, dict)
            and all(isinstance(session.get(field), str) and session[field]
                    for field in ("source", "agent", "kind", "value"))
            and (agent is None or agent == session["agent"])):
        session_agent = session.get("agent") if isinstance(session.get("agent"), str) else None
        agent = agent or session_agent
        if agent == "codex" and (not codex_binding_matches_title(pane) or codex_session != session["value"]):
            session = None
    else:
        session = None
    key = ("session:{}:{}:{}".format(session.get("source", ""), session.get("kind", ""), session["value"])
           if session else pane_key(pane_id))
    label = agent or "terminal"
    if isinstance(cwd, str) and cwd:
        label = f"{label} · {pathlib.PurePath(cwd).name or cwd}"
    return Conversation(key, label, pane_id, agent, cwd if isinstance(cwd, str) else None, session,
                        pane.get("terminal_id") if isinstance(pane.get("terminal_id"), str) else None,
                        pane.get("terminal_title") if isinstance(pane.get("terminal_title"), str) else None)


def fallback_conversation(pane_id: str) -> Conversation:
    return Conversation(pane_key(pane_id), f"pane {pane_id}", pane_id, None, None, None)


def resolved_conversation(herdr: Herdr, pane: dict[str, Any]) -> Conversation:
    proof = herdr.selected_codex_session(pane) if pane.get("agent") == "codex" else None
    return conversation_for_pane(pane, codex_session=proof)


def new_id(existing: set[str]) -> str:
    while True:
        candidate = uuid.uuid4().hex[:8]
        if candidate not in existing:
            return candidate


def normalize_text(text: str) -> str:
    # Drop control characters (notably ESC, which could end a bracketed paste
    # early) except newlines and tabs, and trim surrounding blank space.
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = "".join(ch for ch in text if ch in "\n\t" or unicodedata.category(ch) != "Cc")
    cleaned = cleaned.strip()
    if not cleaned:
        raise QueueError("prompt text is empty")
    if len(cleaned.encode()) > MAX_ENTRY_BYTES:
        raise QueueError(f"prompt text exceeds {MAX_ENTRY_BYTES} bytes")
    return cleaned


def queue_for(data: dict[str, Any], key: str) -> dict[str, Any] | None:
    return data["queues"].get(key)


def ensure_queue(data: dict[str, Any], conversation: Conversation) -> dict[str, Any]:
    queue = data["queues"].get(conversation.key)
    now = utc_now()
    if queue is None:
        queue = {"created_utc": now, "updated_utc": now, "entries": [], "history": []}
        data["queues"][conversation.key] = queue
    touch_queue(queue, conversation)
    return queue


def touch_queue(queue: dict[str, Any], conversation: Conversation) -> None:
    """Record where the conversation was last seen without bumping updated_utc."""
    queue["label"] = conversation.label
    queue["last_pane_id"] = conversation.pane_id
    if conversation.agent:
        queue["agent"] = conversation.agent
    if conversation.cwd:
        queue["cwd"] = conversation.cwd
    if conversation.session:
        queue["session"] = {
            name: conversation.session.get(name) for name in ("source", "agent", "kind", "value")
        }


def all_entry_ids(data: dict[str, Any]) -> set[str]:
    return {
        entry["id"]
        for queue in data["queues"].values()
        for entry in queue["entries"] + queue["history"]
        if isinstance(entry, dict) and "id" in entry
    }


def find_entry(queue: dict[str, Any], entry_id: str) -> tuple[int, dict[str, Any]]:
    matches = [
        (index, entry)
        for index, entry in enumerate(queue["entries"])
        if entry["id"] == entry_id or entry["id"].startswith(entry_id)
    ]
    exact = [match for match in matches if match[1]["id"] == entry_id]
    if exact:
        return exact[0]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise QueueError(f"no queued prompt {entry_id!r}")
    raise QueueError(f"prompt id {entry_id!r} is ambiguous")


def add_entry(
    data: dict[str, Any], conversation: Conversation, text: str, index: int | None = None
) -> dict[str, Any]:
    text = normalize_text(text)
    queue = ensure_queue(data, conversation)
    now = utc_now()
    entry = {"id": new_id(all_entry_ids(data)), "text": text, "created_utc": now, "updated_utc": now}
    if index is None or index >= len(queue["entries"]):
        queue["entries"].append(entry)
    else:
        queue["entries"].insert(max(index, 0), entry)
    queue["updated_utc"] = now
    return entry


def edit_entry(queue: dict[str, Any], entry_id: str, text: str) -> dict[str, Any]:
    _, entry = find_entry(queue, entry_id)
    normalized = normalize_text(text)
    if normalized != entry["text"]:
        entry["text"] = normalized
        # The changed text has not been pasted, even if the old text was.
        entry.pop("pending", None)
        entry["updated_utc"] = queue["updated_utc"] = utc_now()
    return entry


def remove_entry(queue: dict[str, Any], entry_id: str, reason: str) -> dict[str, Any]:
    index, entry = find_entry(queue, entry_id)
    del queue["entries"][index]
    archived = dict(entry)
    archived.pop("pending", None)
    archived.update({"removed_utc": utc_now(), "reason": reason, "index": index})
    queue["history"].append(archived)
    del queue["history"][:-HISTORY_LIMIT]
    queue["updated_utc"] = archived["removed_utc"]
    return archived


def restore_last(queue: dict[str, Any]) -> dict[str, Any] | None:
    if not queue["history"]:
        return None
    archived = queue["history"].pop()
    index = archived.pop("index", len(queue["entries"]))
    archived.pop("removed_utc", None)
    archived.pop("reason", None)
    queue["entries"].insert(min(max(index, 0), len(queue["entries"])), archived)
    queue["updated_utc"] = utc_now()
    return archived


def move_entry(queue: dict[str, Any], entry_id: str, delta: int) -> int:
    index, entry = find_entry(queue, entry_id)
    target = min(max(index + delta, 0), len(queue["entries"]) - 1)
    if target != index:
        del queue["entries"][index]
        queue["entries"].insert(target, entry)
        queue["updated_utc"] = utc_now()
    return target


def boot_id() -> str | None:
    """Identify a Linux/WSL boot without relying on reusable process IDs."""
    try:
        return pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return None


def input_runtime() -> dict[str, Any]:
    """An insertion belongs to this boot and this server socket's lifetime."""
    path = os.environ.get("HERDR_SOCKET_PATH")
    identity = None
    if path:
        with contextlib.suppress(OSError):
            stat = pathlib.Path(path).stat()
            identity = [stat.st_dev, stat.st_ino, stat.st_ctime_ns]
    return {"boot_id": boot_id(), "socket_path": path, "socket_identity": identity}


def mark_pending(queue: dict[str, Any], entry_id: str, pane_id: str, now: float) -> None:
    _, entry = find_entry(queue, entry_id)
    entry["pending"] = {
        "pane_id": pane_id, "inserted_at": now, "inserted_utc": utc_now(),
        "runtime": input_runtime(),
    }


def clear_pending(queue: dict[str, Any], entry_id: str) -> bool:
    _, entry = find_entry(queue, entry_id)
    return entry.pop("pending", None) is not None


def consume_pending(
    data: dict[str, Any], pane_id: str, now: float, *, conversation_key: str,
) -> list[tuple[str, dict[str, Any]]]:
    """Archive entries inserted into `pane_id` before the grace window."""
    consumed: list[tuple[str, dict[str, Any]]] = []
    for key, queue in data["queues"].items():
        if key != conversation_key:
            continue
        for entry in list(queue["entries"]):
            pending = entry.get("pending")
            if not isinstance(pending, dict) or pending.get("pane_id") != pane_id:
                continue
            if now - float(pending.get("inserted_at", 0)) < SUBMIT_GRACE_SECONDS:
                continue
            consumed.append((key, remove_entry(queue, entry["id"], "submitted")))
    return consumed


def merge_queue(data: dict[str, Any], source_key: str, conversation: Conversation) -> int:
    if source_key == conversation.key:
        return 0
    source = data["queues"].pop(source_key, None)
    if source is None:
        raise QueueError(f"no queue {source_key!r}")
    target = ensure_queue(data, conversation)
    for entry in source["entries"]:
        entry.pop("pending", None)
    target["entries"].extend(source["entries"])
    target["history"] = (target["history"] + source["history"])[-HISTORY_LIMIT:]
    target["updated_utc"] = utc_now()
    return len(source["entries"])


def delete_queue(data: dict[str, Any], key: str) -> None:
    queue = data["queues"].pop(key, None)
    if queue is None:
        raise QueueError(f"no queue {key!r}")
    data["deleted_queues"].append({"key": key, "deleted_utc": utc_now(), "queue": queue})
    del data["deleted_queues"][:-DELETED_QUEUE_LIMIT]


def entry_count(data: dict[str, Any], key: str) -> int:
    queue = queue_for(data, key)
    return len(queue["entries"]) if queue else 0


# --------------------------------------------------------------------------
# Pane caches (non-durable; rebuilt by the startup hook)

COUNTS_CACHE = "reported-counts.json"
PANE_KEYS_CACHE = "pane-keys.json"

# Mirrors the resume commands Herdr uses for official agent integrations.
RESUME_COMMANDS = {
    ("herdr:claude", "claude"): "claude --resume {id}",
    ("herdr:codex", "codex"): "codex resume {id}",
    ("herdr:copilot", "copilot"): "copilot --resume={id}",
    ("herdr:devin", "devin"): "devin --resume {id}",
    ("herdr:droid", "droid"): "droid --resume {id}",
    ("herdr:kimi", "kimi"): "kimi --session {id}",
    ("herdr:pi", "pi"): "pi --session {id}",
    ("herdr:omp", "omp"): "omp --resume={id}",
}


def read_cache(directory: pathlib.Path, name: str) -> dict[str, Any]:
    try:
        cache = json.loads((directory / name).read_text())
    except (OSError, ValueError):
        return {}
    return cache if isinstance(cache, dict) else {}


def write_cache(directory: pathlib.Path, name: str, cache: dict[str, Any]) -> None:
    path = directory / name
    try:
        temporary = path.with_name(f".{path.name}.{os.getpid()}")
        temporary.write_text(json.dumps(cache, sort_keys=True))
        os.replace(temporary, path)
    except OSError:
        pass


def report_count(herdr: Herdr, directory: pathlib.Path, pane_id: str, count: int) -> None:
    cache = read_cache(directory, COUNTS_CACHE)
    # Only clear a token this plugin reported; Herdr drops tokens on restart.
    if cache.get(pane_id, 0) == count:
        return
    try:
        herdr.report_count(pane_id, count)
    except HerdrError:
        return
    cache[pane_id] = count
    write_cache(directory, COUNTS_CACHE, cache)


def remember_pane_key(directory: pathlib.Path, conversation: Conversation) -> None:
    if conversation.session is None:
        return
    cache = read_cache(directory, PANE_KEYS_CACHE)
    if cache.get(conversation.pane_id) != conversation.key:
        cache[conversation.pane_id] = conversation.key
        write_cache(directory, PANE_KEYS_CACHE, cache)


def resume_hint(queue: dict[str, Any]) -> str | None:
    session = queue.get("session")
    if not isinstance(session, dict) or not session.get("value"):
        return None
    template = RESUME_COMMANDS.get((session.get("source"), session.get("agent")))
    return template.format(id=session["value"]) if template else None


def warn_if_ended(store: Store, herdr: Herdr, pane_id: str, current_key: str | None) -> bool:
    """Notify when the conversation last seen in a pane left it with prompts queued.

    `current_key` is the conversation the pane holds now, or None when its agent
    exited or the pane closed.
    """
    cache = read_cache(store.directory, PANE_KEYS_CACHE)
    previous = cache.get(pane_id)
    if not previous or previous == current_key:
        return False
    del cache[pane_id]
    write_cache(store.directory, PANE_KEYS_CACHE, cache)
    queue = queue_for(store.read(), previous)
    count = len(queue["entries"]) if queue else 0
    if not count:
        return False
    plural = "prompt" if count == 1 else "prompts"
    title = f"{queue.get('label', 'Conversation')} ended with {count} queued {plural}"
    hint = resume_hint(queue)
    body = (
        f"Resume with: {hint}" if hint else "Move them from another conversation's queue with o."
    )
    try:
        herdr.notify(title, body)
    except HerdrError:
        return False
    return True


def resolve(herdr: Herdr, pane_id: str, directory: pathlib.Path | None = None) -> Conversation:
    try:
        pane = herdr.pane(pane_id)
    except HerdrError:
        return fallback_conversation(pane_id)
    conversation = resolved_conversation(herdr, pane)
    if directory is not None and pane.get("agent"):
        remember_pane_key(directory, conversation)
    return conversation


def delivery_conversation(herdr: Herdr, expected: Conversation) -> Conversation:
    """Verify native ownership immediately before a user-requested delivery.

    A pane can outlive its conversation. Neither a cached popup identity nor a
    pane-keyed queue proves that the current recipient owns the queued prompt.
    """
    def identity(conversation: Conversation) -> tuple[str, ...] | None:
        session = conversation.session
        if not isinstance(session, dict):
            return None
        fields = tuple(session.get(key) for key in ("source", "agent", "kind", "value"))
        if (any(not isinstance(value, str) or not value for value in fields)
                or conversation.agent != session.get("agent")):
            return None
        return fields

    expected_identity = identity(expected)
    if expected_identity is None:
        raise QueueError("Conversation identity is unavailable; keep this queue and reopen after binding")
    pane = herdr.pane(expected.pane_id)
    current = resolved_conversation(herdr, pane)
    if (pane.get("agent") != expected.agent or current.pane_id != expected.pane_id
            or identity(current) != expected_identity):
        raise QueueError("Conversation changed or is unavailable; reopen the queue before delivery")
    if not current.terminal_id:
        raise QueueError("Live terminal identity is unavailable; queued prompt retained")
    return current


def sync_pane(
    store: Store, herdr: Herdr, pane: dict[str, Any], *, working: bool = False,
) -> Conversation:
    """Recover this exact conversation's saved prompts and refresh its count."""
    conversation = resolved_conversation(herdr, pane)
    if pane.get("agent"):
        remember_pane_key(store.directory, conversation)
    recover_unassigned_drafts(store)
    recover_drafts(store, conversation)
    runtime = input_runtime()
    verified = conversation.session is not None and pane.get("agent") == conversation.agent
    with store.transaction() as data:
        binding = pane.get("agent_session")
        if (conversation.agent == "codex" and isinstance(binding, dict)
                and conversation.session is None):
            # A different idle thread can start working while the old native
            # binding remains. Its input no longer proves submission of the
            # old queued prompt, even within the same server/socket runtime.
            old_key = "session:{}:{}:{}".format(binding.get("source", ""), binding.get("kind", ""),
                                                binding.get("value", ""))
            old_queue = queue_for(data, old_key)
            for entry in old_queue["entries"] if old_queue else []:
                pending = entry.get("pending")
                if isinstance(pending, dict) and pending.get("pane_id") == conversation.pane_id:
                    entry.pop("pending", None)
        queue = queue_for(data, conversation.key)
        if queue is not None:
            touch_queue(queue, conversation)
            for entry in queue["entries"]:
                pending = entry.get("pending")
                if not isinstance(pending, dict):
                    continue
                # A recovered PTY has lost its old unsubmitted input. Retain
                # the queued prompt, but stop waiting for its old submit signal.
                stale = not verified or pending.get("pane_id") != conversation.pane_id
                if "runtime" in pending:
                    stale = stale or pending["runtime"] != runtime
                else:
                    # Older markers have no proof that the original input
                    # still exists. Prefer retaining the queued message.
                    stale = True
                if stale:
                    entry.pop("pending", None)
            if working and verified:
                consume_pending(data, conversation.pane_id, time.time(), conversation_key=conversation.key)
        count = entry_count(data, conversation.key)
    report_count(herdr, store.directory, conversation.pane_id, count)
    return conversation


# --------------------------------------------------------------------------
# Draft files, so an editor session survives a crash


def drafts_dir(store: Store) -> pathlib.Path:
    return store.directory / "drafts"


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError as exc:
        return exc.errno == errno.EPERM
    return True


def process_start(pid: int) -> str | None:
    try:
        # The command name in /proc/PID/stat can contain spaces and parentheses.
        return pathlib.Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError):
        return None


def draft_owner_alive(meta: dict[str, Any]) -> bool:
    owner_boot = meta.get("boot_id")
    current_boot = boot_id()
    if owner_boot and current_boot and owner_boot != current_boot:
        return False
    pid = meta.get("pid", 0)
    if not isinstance(pid, int) or not pid_alive(pid):
        return False
    owner_start = meta.get("process_start")
    current_start = process_start(pid)
    return not owner_start or not current_start or owner_start == current_start


def recover_drafts(store: Store, conversation: Conversation) -> int:
    """Apply drafts left behind by an editor session that never returned."""
    directory = drafts_dir(store)
    if not directory.is_dir():
        return 0
    recovered = 0
    for meta_path in sorted(directory.glob("*.json")):
        try:
            meta = json.loads(meta_path.read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(meta, dict) or meta.get("key") != conversation.key or draft_owner_alive(meta):
            continue
        draft_path = meta_path.with_suffix(".md")
        try:
            text = draft_path.read_text()
        except OSError:
            text = ""
        with store.transaction() as data:
            queue = queue_for(data, conversation.key)
            try:
                entry_id = meta.get("entry_id")
                add_as_new = False
                if entry_id and queue is not None and any(e["id"] == entry_id for e in queue["entries"]):
                    current = find_entry(queue, entry_id)[1]
                    if normalize_text(text) != current["text"]:
                        if current["text"] == meta.get("initial"):
                            edit_entry(queue, entry_id, text)
                            recovered += 1
                        else:
                            # Another writer edited this entry after the
                            # abandoned editor opened. Keep both versions.
                            add_as_new = True
                else:
                    add_as_new = bool(text.strip() and text.strip() != meta.get("initial", "").strip())
                if add_as_new and not any(
                    entry.get("recovered_draft") == meta_path.stem
                    for entry in (queue["entries"] + queue["history"] if queue else [])
                ):
                    entry = add_entry(data, conversation, text)
                    entry["recovered_draft"] = meta_path.stem
                    recovered += 1
            except QueueError:
                # Invalid/oversized drafts remain available for manual rescue.
                continue
        for path in (draft_path, meta_path):
            with contextlib.suppress(OSError):
                path.unlink()
    return recovered


def recover_unassigned_drafts(store: Store) -> int:
    """Expose abandoned drafts under their original unbound pane queue only."""
    keys = set()
    for meta_path in drafts_dir(store).glob("*.json"):
        try:
            meta = json.loads(meta_path.read_text())
        except (OSError, ValueError):
            continue
        key = meta.get("key") if isinstance(meta, dict) else None
        if isinstance(key, str) and key.startswith("pane:") and key[5:]:
            keys.add(key)
    return sum(recover_drafts(store, fallback_conversation(key[5:])) for key in sorted(keys))


class DraftFile:
    """On-disk copy of an open edit, applied by recover_drafts after a crash."""

    SAVE_INTERVAL_SECONDS = 1.0

    def __init__(self, store: Store, conversation: Conversation, initial: str, entry_id: str | None):
        directory = drafts_dir(store)
        directory.mkdir(parents=True, exist_ok=True)
        stem = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.text_path = directory / f"{stem}.md"
        self.meta_path = directory / f"{stem}.json"
        self.saved = initial
        self.saved_at = 0.0
        self.unassigned = conversation.session is None
        atomic_write(self.text_path, initial.encode())
        atomic_write(
            self.meta_path,
            json.dumps({
                "key": conversation.key, "entry_id": entry_id, "pid": os.getpid(), "initial": initial,
                "boot_id": boot_id(), "process_start": process_start(os.getpid()),
            }).encode(),
        )

    def unassign(self, pane_id: str) -> None:
        if self.unassigned:
            return
        meta = json.loads(self.meta_path.read_text())
        meta.update(key=pane_key(pane_id), entry_id=None)
        atomic_write(self.meta_path, json.dumps(meta).encode())
        self.unassigned = True

    def save(self, text: str, now: float, force: bool = False) -> None:
        if text == self.saved or (not force and now - self.saved_at < self.SAVE_INTERVAL_SECONDS):
            return
        with contextlib.suppress(OSError):
            atomic_write(self.text_path, text.encode())
            self.saved, self.saved_at = text, now

    def remove(self) -> None:
        for path in (self.text_path, self.meta_path):
            with contextlib.suppress(OSError):
                path.unlink()


# --------------------------------------------------------------------------
# Terminal UI


def display_width(text: str) -> int:
    return sum(2 if unicodedata.east_asian_width(ch) in "WF" else 1 for ch in text)


def clip(text: str, width: int) -> str:
    if width <= 0:
        return ""
    if display_width(text) <= width:
        return text
    out, used = [], 0
    for ch in text:
        size = 2 if unicodedata.east_asian_width(ch) in "WF" else 1
        if used + size > width - 1:
            break
        out.append(ch)
        used += size
    return "".join(out) + "…"


def wrap(text: str, width: int) -> list[str]:
    lines: list[str] = []
    for paragraph in text.split("\n"):
        current, used = "", 0
        for ch in paragraph.replace("\t", "    "):
            size = 2 if unicodedata.east_asian_width(ch) in "WF" else 1
            if used + size > width:
                lines.append(current)
                current, used = "", 0
            current += ch
            used += size
        lines.append(current)
    return lines


def char_width(ch: str) -> int:
    return 2 if unicodedata.east_asian_width(ch) in "WF" else 1


class EditBuffer:
    """Multi-line prompt text and cursor, independent of curses."""

    def __init__(self, text: str = ""):
        self.lines = text.split("\n") if text else [""]
        self.row = len(self.lines) - 1
        self.col = len(self.lines[-1])
        self.goal_x: int | None = None

    @property
    def text(self) -> str:
        return "\n".join(self.lines)

    def insert(self, chars: str) -> None:
        for ch in chars.replace("\r\n", "\n").replace("\r", "\n"):
            if ch == "\n":
                self.newline()
            elif ch == "\t":
                self.insert("    ")
            elif unicodedata.category(ch) != "Cc":
                line = self.lines[self.row]
                self.lines[self.row] = line[: self.col] + ch + line[self.col :]
                self.col += 1
        self.goal_x = None

    def newline(self) -> None:
        line = self.lines[self.row]
        self.lines[self.row] = line[: self.col]
        self.lines.insert(self.row + 1, line[self.col :])
        self.row += 1
        self.col = 0
        self.goal_x = None

    def backspace(self) -> None:
        if self.col > 0:
            line = self.lines[self.row]
            self.lines[self.row] = line[: self.col - 1] + line[self.col :]
            self.col -= 1
        elif self.row > 0:
            self.col = len(self.lines[self.row - 1])
            self.lines[self.row - 1] += self.lines.pop(self.row)
            self.row -= 1
        self.goal_x = None

    def delete(self) -> None:
        line = self.lines[self.row]
        if self.col < len(line):
            self.lines[self.row] = line[: self.col] + line[self.col + 1 :]
        elif self.row < len(self.lines) - 1:
            self.lines[self.row] += self.lines.pop(self.row + 1)
        self.goal_x = None

    def left(self) -> None:
        if self.col > 0:
            self.col -= 1
        elif self.row > 0:
            self.row -= 1
            self.col = len(self.lines[self.row])
        self.goal_x = None

    def right(self) -> None:
        if self.col < len(self.lines[self.row]):
            self.col += 1
        elif self.row < len(self.lines) - 1:
            self.row += 1
            self.col = 0
        self.goal_x = None

    def home(self) -> None:
        self.col = 0
        self.goal_x = None

    def end(self) -> None:
        self.col = len(self.lines[self.row])
        self.goal_x = None

    def layout(self, width: int) -> list[tuple[int, int, int]]:
        """Soft-wrap into display rows of (line index, start col, end col)."""
        width = max(width, 2)
        rows: list[tuple[int, int, int]] = []
        for index, line in enumerate(self.lines):
            start, used = 0, 0
            for col, ch in enumerate(line):
                size = char_width(ch)
                if used + size > width:
                    rows.append((index, start, col))
                    start, used = col, 0
                used += size
            rows.append((index, start, len(line)))
        return rows

    def cursor(self, rows: list[tuple[int, int, int]]) -> tuple[int, int]:
        """Return the cursor's display row and column."""
        for position, (index, start, end) in enumerate(rows):
            last_segment = position + 1 == len(rows) or rows[position + 1][0] != index
            if index == self.row and start <= self.col and (self.col < end or (self.col == end and last_segment)):
                return position, display_width(self.lines[index][start : self.col])
        return len(rows) - 1, 0

    def vertical(self, delta: int, width: int) -> None:
        rows = self.layout(width)
        position, x = self.cursor(rows)
        goal = x if self.goal_x is None else self.goal_x
        target = position + delta
        if target < 0:
            self.row, self.col = 0, 0
        elif target >= len(rows):
            self.row, self.col = len(self.lines) - 1, len(self.lines[-1])
        else:
            index, start, end = rows[target]
            line = self.lines[index]
            col, used = start, 0
            while col < end and used + char_width(line[col]) <= goal:
                used += char_width(line[col])
                col += 1
            last_segment = target + 1 == len(rows) or rows[target + 1][0] != index
            if col == end and not last_segment and end > start:
                col = end - 1
            self.row, self.col = index, col
        self.goal_x = goal


EDITOR_HINTS = [("esc", "save"), ("enter", "new line"), ("ctrl+c", "discard")]


def edit_prompt(screen, store: Store, conversation: Conversation, title: str, initial: str, entry_id: str | None,
                *, owner_check=None) -> str | None:
    """Edit a prompt in the popup. Esc saves; ctrl+c discards.

    Returns the new text, or None when it was discarded, left empty, or unchanged.
    """
    import curses

    buffer = EditBuffer(initial)
    draft = DraftFile(store, conversation, initial, entry_id)
    if owner_check and not owner_check():
        draft.unassign(conversation.pane_id)
    scroll = 0
    curses.curs_set(1)
    try:
        while True:
            height, width = screen.getmaxyx()
            text_width = max(width - 2, 2)
            text_height = max(height - 3, 1)
            rows = buffer.layout(text_width)
            position, x = buffer.cursor(rows)
            scroll = min(max(scroll, position - text_height + 1), position)
            screen.erase()
            screen.addstr(0, 0, clip(f" {title} — {conversation.label}", width - 1), curses.A_BOLD)
            for offset, (index, start, end) in enumerate(rows[scroll : scroll + text_height]):
                screen.addstr(1 + offset, 1, buffer.lines[index][start:end])
            hints = "  ".join(f"{key} {label}" for key, label in EDITOR_HINTS)
            screen.addstr(height - 1, 0, clip(" " + hints, width - 1), curses.A_DIM)
            screen.move(1 + position - scroll, min(1 + x, width - 1))
            screen.refresh()

            key = screen.get_wch()
            # Drain queued input (a paste) before redrawing.
            screen.nodelay(True)
            try:
                while True:
                    if key in ("\x1b", "\x11"):
                        if owner_check and not owner_check():
                            draft.unassign(conversation.pane_id)
                        return finish_edit(buffer.text, initial)
                    if key == "\x03":
                        return None
                    apply_edit_key(buffer, key, text_width)
                    try:
                        key = screen.get_wch()
                    except curses.error:
                        break
            finally:
                screen.nodelay(False)
            now = time.time()
            if buffer.text != draft.saved and now - draft.saved_at >= draft.SAVE_INTERVAL_SECONDS:
                if owner_check and not owner_check():
                    draft.unassign(conversation.pane_id)
                draft.save(buffer.text, now)
    finally:
        draft.remove()
        curses.curs_set(0)


def finish_edit(text: str, initial: str) -> str | None:
    if not text.strip() or text.strip() == initial.strip():
        return None
    return text


def apply_edit_key(buffer: EditBuffer, key, width: int) -> None:
    import curses

    if isinstance(key, str):
        if key in ("\n", "\r"):
            buffer.newline()
        elif key in ("\x7f", "\x08"):
            buffer.backspace()
        elif key == "\x01":
            buffer.home()
        elif key == "\x05":
            buffer.end()
        else:
            buffer.insert(key)
        return
    actions = {
        curses.KEY_ENTER: buffer.newline,
        curses.KEY_BACKSPACE: buffer.backspace,
        curses.KEY_DC: buffer.delete,
        curses.KEY_LEFT: buffer.left,
        curses.KEY_RIGHT: buffer.right,
        curses.KEY_HOME: buffer.home,
        curses.KEY_END: buffer.end,
        curses.KEY_UP: lambda: buffer.vertical(-1, width),
        curses.KEY_DOWN: lambda: buffer.vertical(1, width),
    }
    action = actions.get(key)
    if action:
        action()


QUEUE_HINTS = [
    ("enter", "insert"),
    ("s", "send now"),
    ("a", "add"),
    ("e", "edit"),
    ("d", "delete"),
    ("J/K", "move"),
    ("u", "undo"),
    ("p", "unmark"),
    ("o", "other queues"),
    ("q/ctrl+q", "close"),
]
# ctrl+q toggles the popup closed (it is the suggested open key); ctrl+c also
# closes because raw mode stops it from interrupting.
CLOSE_KEYS = ("\x11", "\x03")
OTHER_HINTS = [("enter", "move into this conversation"), ("x", "delete queue"), ("esc", "back")]


class QueueApp:
    def __init__(self, store: Store, herdr: Herdr, pane_id: str):
        self.store = store
        self.herdr = herdr
        self.pane_id = pane_id
        self.conversation = fallback_conversation(pane_id)
        self.is_agent = False
        self.entries: list[dict[str, Any]] = []
        self.history_size = 0
        self.others: list[tuple[str, dict[str, Any]]] = []
        self.selected = 0
        self.other_selected = 0
        self.mode = "queue"
        self.message = ""
        self.confirm_delete: str | None = None
        self.editor_unassigned = False

    # data ---------------------------------------------------------------

    def load(self) -> None:
        try:
            pane = self.herdr.pane(self.pane_id)
            self.conversation = resolved_conversation(self.herdr, pane)
            self.is_agent = bool(pane.get("agent"))
            if self.is_agent:
                remember_pane_key(self.store.directory, self.conversation)
        except HerdrError as exc:
            self.conversation = fallback_conversation(self.pane_id)
            self.message = f"Herdr unavailable ({exc.code}); showing the pane queue"
        with self.store.transaction() as data:
            queue = queue_for(data, self.conversation.key)
            if queue is not None:
                touch_queue(queue, self.conversation)
        recovered = recover_unassigned_drafts(self.store) + recover_drafts(self.store, self.conversation)
        if recovered:
            self.message = f"Recovered {recovered} unsaved draft(s)"
        self.refresh()

    def refresh(self) -> None:
        data = self.store.read()
        if self.store.notices:
            self.message = self.store.notices.pop()
        queue = queue_for(data, self.conversation.key)
        self.entries = copy.deepcopy(queue["entries"]) if queue else []
        self.history_size = len(queue["history"]) if queue else 0
        self.others = sorted(
            ((key, q) for key, q in data["queues"].items() if key != self.conversation.key and q["entries"]),
            key=lambda item: item[1].get("updated_utc", ""),
            reverse=True,
        )
        self.selected = min(self.selected, max(len(self.entries) - 1, 0))
        self.other_selected = min(self.other_selected, max(len(self.others) - 1, 0))
        try:
            current = resolved_conversation(self.herdr, self.herdr.pane(self.pane_id))
            count = len(self.entries) if current.key == self.conversation.key else 0
        except HerdrError:
            count = 0
        report_count(self.herdr, self.store.directory, self.pane_id, count)

    def current(self) -> dict[str, Any] | None:
        return self.entries[self.selected] if self.entries else None

    def mutate(self, action) -> Any:
        with self.store.transaction() as data:
            result = action(data)
        self.refresh()
        return result

    # actions ------------------------------------------------------------

    def add(self, screen) -> None:
        text = self.with_editor(screen, "", None)
        if text is None:
            self.message = "Nothing added"
            return
        target = self.editor_destination()
        index = self.selected + 1 if self.entries and target.key == self.conversation.key else None
        self.mutate(lambda data: add_entry(data, target, text, index))
        self.selected = index if index is not None else 0
        self.message = "Added" if target.key == self.conversation.key else "Saved separately under Other; conversation changed"

    def edit(self, screen) -> None:
        entry = self.current()
        if not entry:
            return
        text = self.with_editor(screen, entry["text"], entry["id"])
        if text is None:
            self.message = "Unchanged"
            return
        target = self.editor_destination()
        if target.key == self.conversation.key:
            self.mutate(lambda data: edit_entry(ensure_queue(data, target), entry["id"], text))
            self.message = "Saved"
        else:
            self.mutate(lambda data: add_entry(data, target, text))
            self.message = "Saved separately under Other; original prompt unchanged"

    def delete(self) -> None:
        entry = self.current()
        if not entry:
            return
        self.mutate(lambda data: remove_entry(ensure_queue(data, self.conversation), entry["id"], "deleted"))
        self.message = "Deleted (u to undo)"

    def undo(self) -> None:
        restored = self.mutate(lambda data: restore_last(ensure_queue(data, self.conversation)))
        if restored is None:
            self.message = "Nothing to undo"
            return
        ids = [entry["id"] for entry in self.entries]
        self.selected = ids.index(restored["id"]) if restored["id"] in ids else 0
        self.message = "Restored"

    def move(self, delta: int) -> None:
        entry = self.current()
        if entry:
            self.selected = self.mutate(lambda data: move_entry(ensure_queue(data, self.conversation), entry["id"], delta))

    def unmark(self) -> None:
        entry = self.current()
        if entry and self.mutate(lambda data: clear_pending(ensure_queue(data, self.conversation), entry["id"])):
            self.message = "No longer waiting for submit"

    def insert(self) -> bool:
        entry = self.current()
        if not entry:
            return False
        try:
            current = delivery_conversation(self.herdr, self.conversation)
            self.herdr.paste(self.pane_id, entry["text"], expected=current)
        except (HerdrError, QueueError) as exc:
            self.message = f"Insert failed: {exc}"
            return False
        self.mutate(lambda data: mark_pending(ensure_queue(data, self.conversation), entry["id"], self.pane_id, time.time()))
        return True

    def send(self) -> bool:
        entry = self.current()
        if not entry:
            return False
        try:
            current = delivery_conversation(self.herdr, self.conversation)
            self.herdr.submit(self.pane_id, entry["text"], bool(current.agent), expected=current)
        except (HerdrError, QueueError) as exc:
            self.message = f"Send failed: {exc}"
            return False
        self.mutate(lambda data: remove_entry(ensure_queue(data, self.conversation), entry["id"], "sent"))
        return True

    def adopt_other(self) -> None:
        if not self.others:
            return
        delivery_conversation(self.herdr, self.conversation)
        key, _ = self.others[self.other_selected]
        moved = self.mutate(lambda data: merge_queue(data, key, self.conversation))
        self.message = f"Moved {moved} prompt(s) into this conversation"
        self.mode = "queue"

    def delete_other(self) -> None:
        if not self.others:
            return
        key, _ = self.others[self.other_selected]
        if self.confirm_delete != key:
            self.confirm_delete = key
            self.message = "Press x again to delete that whole queue"
            return
        self.confirm_delete = None
        self.mutate(lambda data: delete_queue(data, key))
        self.message = "Queue deleted (kept in deleted_queues of queues.json)"

    def with_editor(self, screen, initial: str, entry_id: str | None) -> str | None:
        title = "Edit prompt" if entry_id else "Add prompt"
        self.editor_unassigned = False

        def owner_check():
            self.editor_destination()
            return not self.editor_unassigned

        return edit_prompt(screen, self.store, self.conversation, title, initial, entry_id,
                           owner_check=owner_check)

    def editor_destination(self) -> Conversation:
        if not self.editor_unassigned:
            try:
                return delivery_conversation(self.herdr, self.conversation)
            except (QueueError, HerdrError):
                self.editor_unassigned = True
        return fallback_conversation(self.pane_id)

    # drawing ------------------------------------------------------------

    def draw(self, screen) -> dict[int, int]:
        import curses

        screen.erase()
        height, width = screen.getmaxyx()
        rows: dict[int, int] = {}
        if height < 6 or width < 20:
            screen.addstr(0, 0, clip("Popup too small", width - 1))
            screen.refresh()
            return rows
        if self.mode == "queue":
            count = len(self.entries)
            title = f" Prompt queue — {self.conversation.label}"
            right = f"{count} queued "
            screen.addstr(0, 0, clip(title, width - len(right) - 1), curses.A_BOLD)
            screen.addstr(0, max(width - len(right) - 1, 0), right)
            subtitle = (
                f" session {self.conversation.session['value'][:8]}" if self.conversation.session else f" pane {self.pane_id}"
            )
            screen.addstr(1, 0, clip(subtitle, width - 1), curses.A_DIM)
            list_height = max(3, (height - 4) // 2)
            if not self.entries:
                screen.addstr(3, 2, clip("Queue is empty. Press a to add a prompt.", width - 3), curses.A_DIM)
            top = max(0, self.selected - list_height + 1)
            for row, index in enumerate(range(top, min(top + list_height, count))):
                entry = self.entries[index]
                marker = "⏳ " if entry.get("pending") else ""
                first_line = entry["text"].split("\n", 1)[0]
                more = " …" if "\n" in entry["text"] else ""
                line = f" {index + 1:>2}  {marker}{first_line}{more}"
                attr = curses.A_REVERSE if index == self.selected else curses.A_NORMAL
                screen.addstr(2 + row, 0, clip(line, width - 1).ljust(width - 1), attr)
                rows[2 + row] = index
            preview_top = 2 + list_height + 1
            screen.hline(preview_top - 1, 0, curses.ACS_HLINE, width)
            entry = self.current()
            if entry:
                preview = wrap(entry["text"], width - 3)
                if entry.get("pending"):
                    preview = ["(inserted — removed when the agent starts working; p to unmark)", ""] + preview
                for offset, line in enumerate(preview[: height - preview_top - 2]):
                    screen.addstr(preview_top + offset, 1, line)
            hints = QUEUE_HINTS
        else:
            screen.addstr(0, 0, clip(" Other conversations' queues", width - 1), curses.A_BOLD)
            screen.addstr(1, 0, clip(f" moving into: {self.conversation.label}", width - 1), curses.A_DIM)
            if not self.others:
                screen.addstr(3, 2, clip("No other queues have prompts.", width - 3), curses.A_DIM)
            top = max(0, self.other_selected - (height - 6))
            for row, index in enumerate(range(top, min(len(self.others), top + height - 5))):
                key, queue = self.others[index]
                where = queue.get("last_pane_id", "?")
                line = (
                    f" {len(queue['entries']):>3}  {queue.get('label', key)}  "
                    f"[{where}, {queue.get('updated_utc', '')[:16].replace('T', ' ')}]"
                )
                attr = curses.A_REVERSE if index == self.other_selected else curses.A_NORMAL
                screen.addstr(2 + row, 0, clip(line, width - 1).ljust(width - 1), attr)
                rows[2 + row] = index
            hints = OTHER_HINTS
        hint_text = "  ".join(f"{key} {label}" for key, label in hints)
        screen.addstr(height - 2, 0, clip(" " + self.message, width - 1), curses.A_BOLD)
        screen.addstr(height - 1, 0, clip(" " + hint_text, width - 1), curses.A_DIM)
        screen.refresh()
        return rows

    # loop ---------------------------------------------------------------

    def run(self, screen) -> None:
        import curses

        curses.curs_set(0)
        # Raw mode so ctrl+q (otherwise XON flow control) reaches the popup.
        curses.raw()
        with contextlib.suppress(curses.error):
            curses.use_default_colors()
        curses.mousemask(curses.ALL_MOUSE_EVENTS | curses.REPORT_MOUSE_POSITION)
        curses.mouseinterval(300)
        screen.keypad(True)
        self.load()
        while True:
            rows = self.draw(screen)
            key = screen.get_wch()
            if key == curses.KEY_RESIZE:
                continue
            if key != "x":
                self.confirm_delete = None
                self.message = ""
            try:
                if self.handle_key(screen, key, rows):
                    return
            except (QueueError, HerdrError) as exc:
                self.message = str(exc)

    def handle_key(self, screen, key, rows: dict[int, int]) -> bool:
        """Apply one key press; return True when the popup should close."""
        import curses

        if key in CLOSE_KEYS:
            return True
        if self.mode == "queue":
            if key in ("q", "\x1b"):
                return True
            if key in ("\n", "\r", "i", curses.KEY_ENTER):
                return self.insert()
            elif key == "s":
                return self.send()
            elif key == "a":
                self.add(screen)
            elif key == "e":
                self.edit(screen)
            elif key in ("d", curses.KEY_DC):
                self.delete()
            elif key == "u":
                self.undo()
            elif key == "p":
                self.unmark()
            elif key == "K":
                self.move(-1)
            elif key == "J":
                self.move(1)
            elif key in ("k", curses.KEY_UP):
                self.selected = max(self.selected - 1, 0)
            elif key in ("j", curses.KEY_DOWN):
                self.selected = min(self.selected + 1, max(len(self.entries) - 1, 0))
            elif key == "o":
                self.mode = "others"
            elif key == "r":
                self.refresh()
            elif key == curses.KEY_MOUSE:
                if self.handle_mouse(rows, "selected"):
                    return self.insert()
        else:
            if key in ("\x1b", "o", "q"):
                self.mode = "queue"
            elif key in ("\n", "\r", curses.KEY_ENTER):
                self.adopt_other()
            elif key == "x":
                self.delete_other()
            elif key in ("k", curses.KEY_UP):
                self.other_selected = max(self.other_selected - 1, 0)
            elif key in ("j", curses.KEY_DOWN):
                self.other_selected = min(self.other_selected + 1, max(len(self.others) - 1, 0))
            elif key == curses.KEY_MOUSE:
                if self.handle_mouse(rows, "other_selected"):
                    self.adopt_other()
        return False

    def handle_mouse(self, rows: dict[int, int], attribute: str) -> bool:
        """Select the clicked row; return True for a double click."""
        import curses

        try:
            _, _, y, _, state = curses.getmouse()
        except curses.error:
            return False
        if state & curses.BUTTON4_PRESSED:
            setattr(self, attribute, max(getattr(self, attribute) - 1, 0))
            return False
        if state & getattr(curses, "BUTTON5_PRESSED", 0):
            limit = len(self.entries if attribute == "selected" else self.others) - 1
            setattr(self, attribute, min(getattr(self, attribute) + 1, max(limit, 0)))
            return False
        if y in rows:
            setattr(self, attribute, rows[y])
            return bool(state & curses.BUTTON1_DOUBLE_CLICKED)
        return False


# --------------------------------------------------------------------------
# Commands


def context_pane() -> str | None:
    raw = os.environ.get("HERDR_PLUGIN_CONTEXT_JSON")
    if raw:
        with contextlib.suppress(ValueError):
            context = json.loads(raw)
            if isinstance(context, dict) and context.get("focused_pane_id"):
                return str(context["focused_pane_id"])
    return None


def target_pane(args: argparse.Namespace) -> str:
    pane = (
        getattr(args, "pane", None)
        or os.environ.get("PROMPT_QUEUE_PANE")
        or context_pane()
        or os.environ.get("HERDR_PANE_ID")
    )
    if not pane:
        raise QueueError("no target pane; pass --pane")
    return pane


def cmd_ui(args: argparse.Namespace) -> int:
    import curses

    os.environ.setdefault("ESCDELAY", "25")
    app = QueueApp(Store(args.state_dir), Herdr(), target_pane(args))
    curses.wrapper(app.run)
    return 0


def cmd_open(args: argparse.Namespace) -> int:
    pane = target_pane(args)
    herdr_bin = os.environ.get("HERDR_BIN_PATH") or "herdr"
    completed = subprocess.run(
        [
            herdr_bin, "plugin", "pane", "open",
            "--plugin", PLUGIN_ID,
            "--entrypoint", PANE_ENTRYPOINT,
            "--placement", "popup",
            "--width", args.width,
            "--height", args.height,
            "--env", f"PROMPT_QUEUE_PANE={pane}",
        ],
        check=False,
    )
    return completed.returncode


def cmd_hook(args: argparse.Namespace) -> int:
    raw = os.environ.get("HERDR_PLUGIN_EVENT_JSON")
    if not raw:
        return 0
    try:
        event = json.loads(raw)
    except ValueError:
        return 0
    if not isinstance(event, dict):
        return 0
    data_section = event.get("data") if isinstance(event.get("data"), dict) else event
    pane_id = data_section.get("pane_id")
    if not isinstance(pane_id, str):
        return 0
    store = Store(args.state_dir)
    if not store.path.exists() and not drafts_dir(store).is_dir():
        return 0
    event_name = str(event.get("event") or os.environ.get("HERDR_PLUGIN_EVENT") or "")
    herdr = Herdr()
    if event_name == "pane.closed":
        warn_if_ended(store, herdr, pane_id, None)
        return 0
    try:
        pane = herdr.pane(pane_id)
        # Herdr keeps the session binding after the agent exits so it can resume
        # it, so an agent that left the pane also ends the conversation here.
        agent_left = event_name == "pane.agent_detected" and not pane.get("agent")
        current = resolved_conversation(herdr, pane)
        warn_if_ended(store, herdr, pane_id, None if agent_left else current.key)
        sync_pane(store, herdr, pane, working=data_section.get("agent_status") == "working")
    except (HerdrError, QueueError):
        pass
    return 0


def cmd_startup(args: argparse.Namespace) -> int:
    store = Store(args.state_dir)
    for name in (COUNTS_CACHE, PANE_KEYS_CACHE):
        with contextlib.suppress(FileNotFoundError):
            (store.directory / name).unlink()
    if not store.path.exists() and not drafts_dir(store).is_dir():
        return 0
    recover_unassigned_drafts(store)
    herdr = Herdr()
    for pane in herdr.panes():
        with contextlib.suppress(HerdrError, QueueError):
            sync_pane(store, herdr, pane)
    return 0


def print_queue(conversation: Conversation, queue: dict[str, Any] | None, as_json: bool) -> None:
    entries = queue["entries"] if queue else []
    if as_json:
        print(json.dumps({"key": conversation.key, "label": conversation.label, "entries": entries}, indent=2, ensure_ascii=False))
        return
    print(f"{conversation.label} ({conversation.key}): {len(entries)} queued")
    for index, entry in enumerate(entries, 1):
        marker = " [inserted]" if entry.get("pending") else ""
        first_line = entry["text"].split("\n", 1)[0]
        print(f"  {index:>2}. {entry['id']}{marker}  {first_line}")


def cmd_list(args: argparse.Namespace) -> int:
    store = Store(args.state_dir)
    data = store.read()
    if args.all:
        if args.json:
            print(json.dumps(data["queues"], indent=2, ensure_ascii=False))
            return 0
        for key, queue in sorted(data["queues"].items()):
            conversation = Conversation(key, queue.get("label", key), queue.get("last_pane_id", "?"), None, None, None)
            print_queue(conversation, queue, False)
        return 0
    conversation = resolve(Herdr(), target_pane(args))
    print_queue(conversation, queue_for(data, conversation.key), args.json)
    return 0


def read_text_argument(parts: list[str]) -> str:
    if parts == ["-"]:
        return sys.stdin.read()
    return " ".join(parts)


def cmd_add(args: argparse.Namespace) -> int:
    store, herdr = Store(args.state_dir), Herdr()
    conversation = resolve(herdr, target_pane(args), store.directory)
    text = read_text_argument(args.text)
    with store.transaction() as data:
        entry = add_entry(data, conversation, text, 0 if args.top else None)
        count = entry_count(data, conversation.key)
    report_count(herdr, store.directory, conversation.pane_id, count)
    print(entry["id"])
    return 0


def with_conversation_queue(args: argparse.Namespace, action, *, herdr: Herdr | None = None) -> Any:
    store, herdr = Store(args.state_dir), herdr if herdr is not None else Herdr()
    conversation = resolve(herdr, target_pane(args), store.directory)
    with store.transaction() as data:
        queue = queue_for(data, conversation.key)
        if queue is None:
            raise QueueError(f"no queue for {conversation.label}")
        result = action(queue, conversation)
        count = len(queue["entries"])
    report_count(herdr, store.directory, conversation.pane_id, count)
    return result


def cmd_edit(args: argparse.Namespace) -> int:
    text = read_text_argument(args.text)
    with_conversation_queue(args, lambda queue, _: edit_entry(queue, args.id, text))
    return 0


def cmd_remove(args: argparse.Namespace) -> int:
    with_conversation_queue(args, lambda queue, _: remove_entry(queue, args.id, "deleted"))
    return 0


def cmd_undo(args: argparse.Namespace) -> int:
    restored = with_conversation_queue(args, lambda queue, _: restore_last(queue))
    if restored is None:
        raise QueueError("nothing to undo")
    print(restored["id"])
    return 0


def cmd_insert(args: argparse.Namespace) -> int:
    herdr = Herdr()
    pane = target_pane(args)

    def action(queue: dict[str, Any], conversation: Conversation) -> None:
        _, entry = find_entry(queue, args.id)
        current = delivery_conversation(herdr, conversation)
        herdr.paste(pane, entry["text"], expected=current)
        mark_pending(queue, entry["id"], pane, time.time())

    with_conversation_queue(args, action, herdr=herdr)
    return 0


def cmd_send(args: argparse.Namespace) -> int:
    herdr = Herdr()
    pane = target_pane(args)
    def action(queue: dict[str, Any], conversation: Conversation) -> None:
        _, entry = find_entry(queue, args.id)
        current = delivery_conversation(herdr, conversation)
        herdr.submit(pane, entry["text"], bool(current.agent), expected=current)
        remove_entry(queue, entry["id"], "sent")

    with_conversation_queue(args, action, herdr=herdr)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prompt-queue", description=__doc__.split("\n\n")[0])
    parser.add_argument("--state-dir", type=pathlib.Path, default=default_state_dir())
    commands = parser.add_subparsers(dest="command", required=True)

    def command(name: str, handler, help_text: str, pane: bool = True) -> argparse.ArgumentParser:
        sub = commands.add_parser(name, help=help_text)
        sub.set_defaults(handler=handler)
        if pane:
            sub.add_argument("--pane", help="target pane (defaults to the current or focused pane)")
        return sub

    command("ui", cmd_ui, "interactive queue editor")
    opener = command("open", cmd_open, "open the queue popup for the focused pane")
    opener.add_argument("--width", default="80%")
    opener.add_argument("--height", default="70%")
    command("hook", cmd_hook, "plugin event hook", pane=False)
    command("startup", cmd_startup, "plugin startup hook", pane=False)
    lister = command("list", cmd_list, "show a conversation's queue")
    lister.add_argument("--json", action="store_true")
    lister.add_argument("--all", action="store_true", help="show every stored queue")
    adder = command("add", cmd_add, "queue a prompt (use - to read stdin)")
    adder.add_argument("--top", action="store_true", help="put it first instead of last")
    adder.add_argument("text", nargs="+")
    editor = command("edit", cmd_edit, "replace a queued prompt's text")
    editor.add_argument("id")
    editor.add_argument("text", nargs="+")
    remover = command("remove", cmd_remove, "remove a queued prompt")
    remover.add_argument("id")
    command("undo", cmd_undo, "restore the most recently removed prompt")
    inserter = command("insert", cmd_insert, "paste a prompt into the agent without submitting")
    inserter.add_argument("id")
    sender = command("send", cmd_send, "submit a prompt now and remove it")
    sender.add_argument("id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except (QueueError, HerdrError) as exc:
        print(f"prompt-queue: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
