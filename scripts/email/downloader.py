#!/usr/bin/env python3
"""Download raw Gmail messages as idempotent .eml files."""

from __future__ import annotations

import argparse
import atexit
import getpass
import hashlib
import imaplib
import json
import os
import re
import shutil
import socket
import sqlite3
import sys
import time
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue
from threading import Event, RLock, Thread
from typing import Any, Callable


IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993
IMAP_TIMEOUT_SECONDS = 30
APP_NAME = "gmail-downloader"
MESSAGE_INDEX_DB_FILENAME = "message_index.sqlite3"
DEFAULT_METADATA_BATCH_SIZE = 1000
MAX_METADATA_BATCH_SIZE = 1000
MIN_METADATA_BATCH_SIZE = 100
METADATA_BATCH_GROW_AFTER = 8
DB_COMMIT_EVERY = 1000
SQLITE_QUERY_CHUNK_SIZE = 900
WORKER_MESSAGE_ATTEMPTS = 4
WORKER_RETRY_BASE_DELAY_SECONDS = 0.4
WORKER_RETRY_MAX_DELAY_SECONDS = 3.0
GMAIL_IMAP_CONNECTION_LIMIT = 15
RESERVED_IMAP_CONNECTIONS = 2
MAIN_IMAP_CONNECTIONS = 1
MAX_DOWNLOAD_WORKERS = max(1, GMAIL_IMAP_CONNECTION_LIMIT - RESERVED_IMAP_CONNECTIONS - MAIN_IMAP_CONNECTIONS)
STATUS_REFRESH_SECONDS = 0.08
STATUS_ANIMATION_SECONDS = 0.25
STATUS_HEARTBEAT_SECONDS = 0.25
ETA_MIN_SAMPLE_SECONDS = 5.0
ETA_MIN_SAMPLE_MESSAGES = 20
ETA_SMOOTHING_ALPHA = 0.35
TRANSFER_RATE_SMOOTHING_ALPHA = 0.45
TRANSFER_RATE_IDLE_SECONDS = 3.0
RAW_FILE_RE = re.compile(r"__(?P<gmail_msg_id>\d+)\.eml$")
RETRYABLE_GMAIL_ERROR_PATTERNS = (
    "too many simultaneous",
    "too many requests",
    "too many concurrent requests for user",
    "rate limit exceeded",
    "user rate limit exceeded",
    "backend error",
    "bad gateway",
    "service unavailable",
    "gateway timeout",
)
RETRYABLE_NETWORK_ERROR_PATTERNS = (
    "timeouterror:",
    "socket.timeout:",
    "connectionreseterror:",
    "connectionabortederror:",
    "brokenpipeerror:",
    "sslerror:",
    "ssleoferror:",
    "imap abort:",
    "socket error:",
    "networkvalidationerror:",
    "downloaded size mismatch",
)


def default_config_path() -> Path:
    env_path = os.environ.get("GMAIL_DOWNLOADER_CONFIG")
    if env_path:
        return Path(env_path).expanduser()

    config_home = os.environ.get("XDG_CONFIG_HOME")
    if config_home:
        return Path(config_home).expanduser() / APP_NAME / "email.json"

    return Path.home() / ".config" / APP_NAME / "email.json"


def default_email_root() -> Path:
    env_path = os.environ.get("GMAIL_DOWNLOADER_EMAILS_DIR")
    if env_path:
        return Path(env_path).expanduser()

    return Path.cwd() / "emails"


def default_download_workers() -> int:
    cpu_count = os.cpu_count() or 1
    machine_workers = max(4, cpu_count * 4)

    env_value = os.environ.get("GMAIL_DOWNLOADER_WORKERS")
    if env_value:
        try:
            requested_workers = int(env_value)
        except ValueError:
            requested_workers = machine_workers
        machine_workers = max(1, requested_workers)

    return max(1, min(machine_workers, MAX_DOWNLOAD_WORKERS))


DEFAULT_CONFIG_PATH = default_config_path()
DEFAULT_EMAIL_ROOT = default_email_root()
DEFAULT_DOWNLOAD_WORKERS = default_download_workers()

APP_PASSWORD_LENGTH = 16
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
FETCH_GMAIL_ID_RE = re.compile(rb"X-GM-MSGID\s+(\d+)")
FETCH_INTERNALDATE_RE = re.compile(rb'INTERNALDATE\s+"([^"]+)"')
FETCH_SIZE_RE = re.compile(rb"RFC822\.SIZE\s+(\d+)")
FETCH_UID_RE = re.compile(rb"\bUID\s+(\d+)")
LIST_RE = re.compile(
    rb"^\((?P<flags>[^)]*)\)\s+(?P<delimiter>NIL|\"(?:\\.|[^\"])*\")\s+(?P<name>.+)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Credentials:
    email: str
    app_password: str


@dataclass(frozen=True)
class Mailbox:
    display_name: str
    select_arg: bytes
    flags: tuple[str, ...]


@dataclass(frozen=True)
class MessageMetadata:
    gmail_msg_id: str
    internal_date: datetime
    rfc822_size: int | None


@dataclass(frozen=True)
class PendingDownload:
    uid: bytes
    metadata: MessageMetadata
    final_path: Path
    mailbox_name: str
    previous_entry: dict[str, Any] | None
    restoring: bool
    repairing_corrupt: bool = False


@dataclass(frozen=True)
class DownloadOutcome:
    pending: PendingDownload
    byte_size: int | None
    sha256: str | None
    downloaded_at: str | None
    error: str | None
    retries: int = 0
    reconnects: int = 0


@dataclass(frozen=True)
class MailboxPlan:
    mailbox: Mailbox
    message_count: int
    uidvalidity: str | None
    state: dict[str, Any]
    sync_mode: str
    start_uid: int | None
    end_uid: int | None
    uids: list[bytes]


@dataclass(frozen=True)
class MetadataBatchJob:
    offset: int
    batch_size: int
    uids: list[bytes]
    future: Future


class CredentialError(RuntimeError):
    """Raised when credential validation should be retried."""


class NetworkValidationError(RuntimeError):
    """Raised when credentials cannot be tested because Gmail is unreachable."""


class LiveStatus:
    def __init__(self) -> None:
        self.dynamic = sys.stdout.isatty() and os.environ.get("TERM", "dumb").lower() != "dumb"
        self.last_lines: list[str] = []
        self.last_update = 0.0
        self.current_message = ""
        self.message_version = 0
        self.rendered_version = 0
        self.animation_frame = 0
        self.animation_generation = 0
        self.animation_thread: Thread | None = None
        self.deferred_thread: Thread | None = None
        self.cursor_hidden = False
        self.lock = RLock()
        atexit.register(self.restore_cursor)

    def update(self, message: str, *, force: bool = False) -> None:
        now = time.monotonic()

        with self.lock:
            self.current_message = message
            self.message_version += 1

            if not force and now - self.last_update < STATUS_REFRESH_SECONDS:
                if self.dynamic:
                    self.schedule_deferred_render_locked()
                return

            self.render_current_locked(now)

    def line(self, message: str = "") -> None:
        with self.lock:
            self.current_message = ""
            self.message_version += 1
            self.rendered_version = self.message_version
            self.animation_generation += 1
            self.clear_dynamic_locked()
            self.show_cursor_locked()
        if message:
            print(message, flush=True)

    def done(self) -> None:
        with self.lock:
            self.current_message = ""
            self.message_version += 1
            self.rendered_version = self.message_version
            self.animation_generation += 1
            if self.dynamic and self.last_lines:
                print()
                self.last_lines = []
            self.show_cursor_locked()

    def restore_cursor(self) -> None:
        with self.lock:
            self.show_cursor_locked()

    def hide_cursor_locked(self) -> None:
        if self.dynamic and not self.cursor_hidden:
            print("\x1b[?25l", end="", flush=True)
            self.cursor_hidden = True

    def show_cursor_locked(self) -> None:
        if self.dynamic and self.cursor_hidden:
            print("\x1b[?25h", end="", flush=True)
            self.cursor_hidden = False

    def clear_dynamic(self) -> None:
        with self.lock:
            self.clear_dynamic_locked()

    def clear_dynamic_locked(self) -> None:
        if not self.dynamic or not self.last_lines:
            return

        print("\r\x1b[2K", end="")
        for _ in range(len(self.last_lines) - 1):
            print("\x1b[1A\r\x1b[2K", end="")
        self.last_lines = []

    def render_dynamic_locked(self, message: str) -> None:
        lines = self.fit_lines(message)
        self.hide_cursor_locked()
        self.clear_dynamic_locked()
        print("\n".join(lines), end="", flush=True)
        self.last_lines = lines

    def render_current_locked(self, now: float | None = None) -> None:
        self.last_update = now if now is not None else time.monotonic()
        self.rendered_version = self.message_version
        self.animation_frame = 0
        self.animation_generation += 1
        if self.dynamic:
            self.render_dynamic_locked(self.render_animation(self.current_message))
            if "..." in self.current_message:
                self.start_animation_locked(self.animation_generation)
        else:
            print(self.current_message, flush=True)

    def schedule_deferred_render_locked(self) -> None:
        if self.deferred_thread is not None and self.deferred_thread.is_alive():
            return

        self.deferred_thread = Thread(target=self.deferred_render, daemon=True)
        self.deferred_thread.start()

    def deferred_render(self) -> None:
        while True:
            with self.lock:
                if (
                    not self.dynamic
                    or not self.current_message
                    or self.message_version == self.rendered_version
                ):
                    self.deferred_thread = None
                    return

                wait_seconds = STATUS_REFRESH_SECONDS - (time.monotonic() - self.last_update)
                if wait_seconds <= 0:
                    self.render_current_locked()
                    self.deferred_thread = None
                    return

            time.sleep(min(wait_seconds, STATUS_REFRESH_SECONDS))

    def render_animation(self, message: str) -> str:
        if "..." not in message:
            return message
        dots = "." * (self.animation_frame + 1)
        return message.replace("...", dots)

    def start_animation_locked(self, generation: int) -> None:
        self.animation_thread = Thread(target=self.animate, args=(generation,), daemon=True)
        self.animation_thread.start()

    def animate(self, generation: int) -> None:
        while True:
            time.sleep(STATUS_ANIMATION_SECONDS)
            with self.lock:
                if (
                    generation != self.animation_generation
                    or not self.dynamic
                    or "..." not in self.current_message
                    or not self.last_lines
                ):
                    return
                self.animation_frame = (self.animation_frame + 1) % 3
                self.render_dynamic_locked(self.render_animation(self.current_message))

    def fit_lines(self, message: str) -> list[str]:
        raw_lines = message.splitlines() or [""]
        if not self.dynamic:
            return raw_lines

        columns = shutil.get_terminal_size((120, 20)).columns
        limit = max(40, columns - 1)
        lines: list[str] = []
        for line in raw_lines:
            if len(line) <= limit:
                lines.append(line)
            else:
                lines.append(line[: limit - 4].rstrip() + " ...")
        return lines


class CompactHelpParser(argparse.ArgumentParser):
    def format_help(self) -> str:
        return "\n".join(
            [
                f"Usage: {self.prog} [--config CONFIG] [--emails-dir DIR]",
                "Download raw Gmail messages to emails/raw as idempotent .eml files.",
                "Options:",
                "  -h, --help                  show this help message and exit",
                "  --config CONFIG             path to credential JSON",
                "  --emails-dir DIR            archive output directory",
                "  --output-dir DIR            alias for --emails-dir",
                f"  parallel downloads:        {DEFAULT_DOWNLOAD_WORKERS}",
                f"  metadata batch size:       {DEFAULT_METADATA_BATCH_SIZE}",
                "Defaults:",
                f"  config:     {DEFAULT_CONFIG_PATH}",
                f"  output dir: {DEFAULT_EMAIL_ROOT}",
                "Environment overrides:",
                "  GMAIL_DOWNLOADER_CONFIG",
                "  GMAIL_DOWNLOADER_EMAILS_DIR",
                "  GMAIL_DOWNLOADER_WORKERS",
            ]
        ) + "\n"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def format_bytes(byte_count: int) -> str:
    value = float(byte_count)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{byte_count} B"


def format_int(value: int) -> str:
    return f"{value:,}"


def format_duration(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "--"

    total_seconds = int(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"


def format_remaining(seconds: float | None) -> str:
    if seconds is None:
        return "estimating"
    return f"about {format_duration(seconds)}"


def progress_bar(done: int, total: int, *, width: int = 18) -> str:
    if total <= 0:
        return "[" + "-" * width + "]"

    done = max(0, min(done, total))
    filled = round(width * done / total)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def progress_percent(done: int, total: int) -> str:
    if total <= 0:
        return "  0%"
    percent = round(100 * max(0, min(done, total)) / total)
    return f"{percent:3d}%"


def elapsed_since(started_at: float) -> float:
    return max(0.001, datetime.now().timestamp() - started_at)


def estimate_remaining(done: int, total: int, sample_done: int, sample_elapsed: float) -> float | None:
    if total > 0 and total <= done:
        return 0.0
    if total <= 0:
        return None
    if done <= 0:
        return None
    if sample_done < ETA_MIN_SAMPLE_MESSAGES or sample_elapsed < ETA_MIN_SAMPLE_SECONDS:
        return None

    rate = sample_done / sample_elapsed
    if rate <= 0:
        return None
    return (total - done) / rate


def load_json(path: Path, default: Any, *, strict: bool = False) -> Any:
    if not path.exists():
        return default

    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError as exc:
        message = f"{path} is not valid JSON: {exc}"
        if strict:
            raise RuntimeError(message) from exc
        print(f"Warning: {message}")
        return default


def write_json_atomic(path: Path, data: Any, *, mode: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")

    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")

        if mode is not None:
            os.chmod(temp_path, mode)

        os.replace(temp_path, path)

        if mode is not None:
            os.chmod(path, mode)
    except OSError as exc:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise RuntimeError(f"Could not write {path}: {exc}") from exc


def cleanup_part_files(email_root: Path) -> int:
    raw_dir = email_root / "raw"
    if not raw_dir.exists():
        return 0

    removed = 0
    for path in raw_dir.glob(".*.part"):
        try:
            path.unlink()
            removed += 1
        except FileNotFoundError:
            pass
        except OSError as exc:
            print(f"Warning: could not remove partial download {path}: {exc}")
    return removed


def run_daemon_task(function: Callable[..., Any], *args: Any) -> Future:
    future: Future = Future()

    def runner() -> None:
        if not future.set_running_or_notify_cancel():
            return

        try:
            future.set_result(function(*args))
        except BaseException as exc:
            future.set_exception(exc)

    Thread(target=runner, daemon=True).start()
    return future


def normalize_app_password(app_password: str) -> str:
    return "".join(app_password.split())


def validate_email(email_address: str) -> str:
    email_address = email_address.strip()
    if not EMAIL_RE.match(email_address):
        raise CredentialError(
            "That does not look like a valid email address. Use the full Gmail address, "
            "for example name@example.com."
        )
    return email_address


def validate_app_password(app_password: str) -> str:
    normalized = normalize_app_password(app_password)
    if len(normalized) != APP_PASSWORD_LENGTH:
        raise CredentialError(
            "Gmail app passwords are 16 characters after removing spaces. "
            "If Google showed it as four groups, paste it as-is and the script will remove spaces."
        )
    return normalized


def credentials_from_config(config: dict[str, Any]) -> Credentials:
    email_address = validate_email(str(config.get("email", "")))
    app_password = validate_app_password(str(config.get("app_password", "")))
    return Credentials(email=email_address, app_password=app_password)


def save_credentials(config_path: Path, credentials: Credentials) -> None:
    write_json_atomic(
        config_path,
        {
            "email": credentials.email,
            "app_password": credentials.app_password,
        },
        mode=0o600,
    )


def prompt_yes_no(question: str, *, default: bool = False) -> bool:
    suffix = " [Y/n]: " if default else " [y/N]: "
    while True:
        answer = input(question + suffix).strip().lower()
        if not answer:
            return default
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please answer yes or no.")


def ask_to_save_credentials(config_path: Path, credentials: Credentials) -> None:
    print("Credentials validated successfully.")
    if prompt_yes_no(f"Save credentials to {config_path} for future runs?"):
        save_credentials(config_path, credentials)
        print(f"Credentials saved to {config_path}")
    else:
        print("Credentials were not saved. You will be prompted again next run.")


def connect_and_login(credentials: Credentials) -> imaplib.IMAP4_SSL:
    try:
        connection = imaplib.IMAP4_SSL(
            IMAP_HOST,
            IMAP_PORT,
            timeout=IMAP_TIMEOUT_SECONDS,
        )
    except (OSError, socket.timeout) as exc:
        raise NetworkValidationError(
            f"Could not connect to {IMAP_HOST}:{IMAP_PORT}. Check your internet connection "
            "and confirm that IMAP is not blocked by your network."
        ) from exc

    try:
        connection.login(credentials.email, credentials.app_password)
    except imaplib.IMAP4.error as exc:
        close_connection(connection)
        detail = decode_error(exc)
        raise CredentialError(
            "Gmail rejected the email/app-password login. Likely causes: the app password "
            "was copied incorrectly, 2-Step Verification is not enabled, IMAP is disabled, "
            f"or Workspace policy blocks app passwords/IMAP. Gmail said: {detail}"
        ) from exc
    except (OSError, socket.timeout, imaplib.IMAP4.abort) as exc:
        close_connection(connection)
        raise NetworkValidationError(
            "The connection to Gmail dropped while testing credentials. "
            "The config file was not changed."
        ) from exc

    return connection


def decode_error(exc: BaseException) -> str:
    if not exc.args:
        return str(exc)
    first = exc.args[0]
    if isinstance(first, bytes):
        return first.decode("utf-8", "replace")
    return str(first)


def prompt_for_credentials(existing_email: str | None = None) -> Credentials:
    while True:
        email_prompt = "Gmail address"
        if existing_email:
            email_prompt += f" [{existing_email}]"
        email_prompt += ": "

        email_input = input(email_prompt).strip()
        email_address = email_input or existing_email or ""
        app_password = getpass.getpass("Gmail app password: ")

        try:
            return Credentials(
                email=validate_email(email_address),
                app_password=validate_app_password(app_password),
            )
        except CredentialError as exc:
            print(f"Credential input error: {exc}\n")


def get_authenticated_connection(config_path: Path) -> tuple[Credentials, imaplib.IMAP4_SSL]:
    raw_config = load_json(config_path, {}, strict=False)
    existing_email = None

    if isinstance(raw_config, dict):
        existing_email_value = raw_config.get("email")
        if isinstance(existing_email_value, str) and existing_email_value.strip():
            existing_email = existing_email_value.strip()

        try:
            credentials = credentials_from_config(raw_config)
        except CredentialError as exc:
            print(f"Config needs attention: {exc}")
        else:
            try:
                connection = connect_and_login(credentials)
            except CredentialError as exc:
                print(f"Saved credentials failed validation: {exc}\n")
            except NetworkValidationError:
                raise
            else:
                return credentials, connection
    elif config_path.exists():
        print(f"Config needs attention: {config_path} must contain a JSON object.")

    while True:
        credentials = prompt_for_credentials(existing_email)
        try:
            connection = connect_and_login(credentials)
        except CredentialError as exc:
            print(f"Credential validation failed: {exc}\n")
            existing_email = credentials.email
            continue
        except NetworkValidationError:
            raise

        ask_to_save_credentials(config_path, credentials)
        return credentials, connection


def close_connection(connection: imaplib.IMAP4_SSL | None) -> None:
    if connection is None:
        return

    try:
        connection.logout()
    except Exception:
        pass


def unquote_imap_string(token: bytes) -> bytes:
    token = token.strip()
    if len(token) >= 2 and token.startswith(b'"') and token.endswith(b'"'):
        inner = token[1:-1]
        inner = inner.replace(b"\\\\", b"\\")
        inner = inner.replace(b'\\"', b'"')
        return inner
    return token


def mailbox_display_name(name_token: bytes) -> str:
    raw_name = unquote_imap_string(name_token)
    return raw_name.decode("utf-8", "replace")


def parse_mailbox_list_line(line: bytes) -> Mailbox | None:
    match = LIST_RE.match(line.strip())
    if not match:
        return None

    flags = tuple(flag.decode("ascii", "replace") for flag in match.group("flags").split())
    if any(flag.lower() == "\\noselect" for flag in flags):
        return None

    name_token = match.group("name").strip()
    if not name_token:
        return None

    return Mailbox(
        display_name=mailbox_display_name(name_token),
        select_arg=name_token,
        flags=flags,
    )


def list_selectable_mailboxes(connection: imaplib.IMAP4_SSL) -> list[Mailbox]:
    status, data = connection.list()
    if status != "OK":
        raise RuntimeError(f"Could not list Gmail mailboxes: {data!r}")

    mailboxes: list[Mailbox] = []
    for item in data:
        if not isinstance(item, bytes):
            continue
        mailbox = parse_mailbox_list_line(item)
        if mailbox is not None:
            mailboxes.append(mailbox)

    mailboxes.sort(key=lambda mailbox: mailbox.display_name.lower())
    return mailboxes


def search_uids(
    connection: imaplib.IMAP4_SSL,
    *,
    start_uid: int | None = None,
    end_uid: int | None = None,
) -> list[bytes]:
    if end_uid is not None and end_uid < 1:
        return []

    if start_uid is None and end_uid is None:
        status, data = connection.uid("SEARCH", None, "ALL")
    elif start_uid is not None and end_uid is not None:
        status, data = connection.uid("SEARCH", None, "UID", f"{start_uid}:{end_uid}")
    elif start_uid is not None:
        status, data = connection.uid("SEARCH", None, "UID", f"{start_uid}:*")
    else:
        status, data = connection.uid("SEARCH", None, "UID", f"1:{end_uid}")

    if status != "OK":
        raise RuntimeError(f"Could not search selected mailbox: {data!r}")
    if not data or not data[0]:
        return []
    return sort_uids_newest_first(data[0].split())


def sort_uids_newest_first(uids: list[bytes]) -> list[bytes]:
    return sorted(uids, key=lambda uid: int(uid), reverse=True)


def combined_fetch_response(data: list[Any]) -> bytes:
    chunks: list[bytes] = []
    for item in data:
        if isinstance(item, bytes):
            chunks.append(item)
        elif isinstance(item, tuple):
            for part in item:
                if isinstance(part, bytes):
                    chunks.append(part)
    return b" ".join(chunks)


def parse_internal_date(raw_value: bytes | None) -> datetime:
    if raw_value is None:
        return utc_now()

    try:
        parsed = datetime.strptime(raw_value.decode("ascii"), "%d-%b-%Y %H:%M:%S %z")
    except ValueError:
        return utc_now()

    return parsed


def parse_metadata_response(response: bytes) -> tuple[bytes, MessageMetadata]:
    uid_match = FETCH_UID_RE.search(response)
    if uid_match is None:
        raise RuntimeError(f"Gmail did not return UID in metadata response: {response!r}")

    gmail_id_match = FETCH_GMAIL_ID_RE.search(response)
    if gmail_id_match is None:
        raise RuntimeError(
            "Gmail did not return X-GM-MSGID. This script requires Gmail IMAP extensions "
            "for stable deduplication."
        )

    internal_date_match = FETCH_INTERNALDATE_RE.search(response)
    size_match = FETCH_SIZE_RE.search(response)

    return (
        uid_match.group(1),
        MessageMetadata(
            gmail_msg_id=gmail_id_match.group(1).decode("ascii"),
            internal_date=parse_internal_date(internal_date_match.group(1) if internal_date_match else None),
            rfc822_size=int(size_match.group(1)) if size_match else None,
        ),
    )


def metadata_response_items(data: list[Any]) -> list[bytes]:
    items: list[bytes] = []
    for item in data:
        if isinstance(item, bytes) and b"X-GM-MSGID" in item:
            items.append(item)
        elif isinstance(item, tuple):
            combined = combined_fetch_response([item])
            if b"X-GM-MSGID" in combined:
                items.append(combined)
    return items


def uid_sequence_set(uids: list[bytes]) -> bytes:
    return b",".join(uids)


def fetch_metadata_batch(connection: imaplib.IMAP4_SSL, uids: list[bytes]) -> dict[bytes, MessageMetadata]:
    if not uids:
        return {}

    status, data = connection.uid("FETCH", uid_sequence_set(uids), "(UID X-GM-MSGID INTERNALDATE RFC822.SIZE)")
    if status != "OK":
        raise RuntimeError(f"Could not fetch metadata batch: {data!r}")

    metadata_by_uid: dict[bytes, MessageMetadata] = {}
    for response in metadata_response_items(data):
        uid, metadata = parse_metadata_response(response)
        metadata_by_uid[uid] = metadata

    missing_uids = [uid.decode("ascii", "replace") for uid in uids if uid not in metadata_by_uid]
    if missing_uids:
        raise RuntimeError(f"Gmail did not return metadata for UIDs: {', '.join(missing_uids)}")

    return metadata_by_uid


def fetch_message_metadata(connection: imaplib.IMAP4_SSL, uid: bytes) -> MessageMetadata:
    return fetch_metadata_batch(connection, [uid])[uid]


def fetch_raw_message(connection: imaplib.IMAP4_SSL, uid: bytes) -> bytes:
    status, data = connection.uid("FETCH", uid, "(BODY.PEEK[])")
    if status != "OK":
        raise RuntimeError(f"Could not fetch raw email for UID {uid.decode('ascii', 'replace')}: {data!r}")

    for item in data:
        if isinstance(item, tuple) and len(item) >= 2 and isinstance(item[1], bytes):
            return item[1]

    raise RuntimeError(f"Gmail returned no raw email body for UID {uid.decode('ascii', 'replace')}")


def filename_for_message(metadata: MessageMetadata) -> str:
    date_part = metadata.internal_date.date().isoformat()
    return f"{date_part}__{metadata.gmail_msg_id}.eml"


def build_existing_file_lookup(raw_dir: Path) -> dict[str, Path]:
    lookup: dict[str, Path] = {}
    if not raw_dir.exists():
        return lookup

    for path in raw_dir.glob("*.eml"):
        match = RAW_FILE_RE.search(path.name)
        if match:
            lookup.setdefault(match.group("gmail_msg_id"), path)
    return lookup


def run_startup_file_tasks(email_root: Path) -> tuple[int, dict[str, Path]]:
    cleanup_future = run_daemon_task(cleanup_part_files, email_root)
    lookup_future = run_daemon_task(build_existing_file_lookup, email_root / "raw")
    try:
        return cleanup_future.result(), lookup_future.result()
    except KeyboardInterrupt:
        cleanup_future.cancel()
        lookup_future.cancel()
        raise


def find_existing_message_file(existing_files: dict[str, Path], gmail_msg_id: str) -> Path | None:
    path = existing_files.get(gmail_msg_id)
    if path is not None and path.exists():
        return path
    if path is not None:
        existing_files.pop(gmail_msg_id, None)
    return None


def message_file_exists(email_root: Path, entry: dict[str, Any]) -> Path | None:
    rel_path = entry.get("path")
    if isinstance(rel_path, str) and rel_path:
        candidate = email_root / rel_path
        if candidate.exists():
            return candidate

    filename = entry.get("filename")
    if isinstance(filename, str) and filename:
        candidate = email_root / "raw" / filename
        if candidate.exists():
            return candidate

    return None


def message_file_needs_repair(path: Path, metadata: MessageMetadata) -> bool:
    try:
        byte_size = path.stat().st_size
    except FileNotFoundError:
        return True

    if byte_size <= 0:
        return True
    if metadata.rfc822_size is not None and byte_size != metadata.rfc822_size:
        return True
    return False


def merge_label(entry: dict[str, Any], label: str) -> bool:
    labels = entry.setdefault("labels", [])
    if not isinstance(labels, list):
        labels = []
        entry["labels"] = labels
    if label not in labels:
        labels.append(label)
        labels.sort()
        return True
    return False


def index_entry_for_file(
    metadata: MessageMetadata,
    path: Path,
    email_root: Path,
    mailbox_name: str,
    *,
    downloaded_at: str | None,
    sha256: str | None = None,
) -> dict[str, Any]:
    relative_path = path.relative_to(email_root).as_posix()
    stat = path.stat()

    entry: dict[str, Any] = {
        "gmail_msg_id": metadata.gmail_msg_id,
        "filename": path.name,
        "path": relative_path,
        "internal_date": metadata.internal_date.isoformat(),
        "labels": [mailbox_name],
        "byte_size": stat.st_size,
        "last_seen_at": iso_now(),
    }

    if sha256 is not None:
        entry["sha256"] = sha256
    if metadata.rfc822_size is not None:
        entry["gmail_rfc822_size"] = metadata.rfc822_size
    if downloaded_at is not None:
        entry["downloaded_at"] = downloaded_at

    return entry


def index_entry_for_download(
    metadata: MessageMetadata,
    path: Path,
    email_root: Path,
    mailbox_name: str,
    *,
    byte_size: int,
    sha256: str,
    downloaded_at: str,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "gmail_msg_id": metadata.gmail_msg_id,
        "filename": path.name,
        "path": path.relative_to(email_root).as_posix(),
        "internal_date": metadata.internal_date.isoformat(),
        "labels": [mailbox_name],
        "byte_size": byte_size,
        "sha256": sha256,
        "downloaded_at": downloaded_at,
        "last_seen_at": iso_now(),
    }

    if metadata.rfc822_size is not None:
        entry["gmail_rfc822_size"] = metadata.rfc822_size

    return entry


def write_raw_message(path: Path, content: bytes) -> tuple[int, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.part")
    digest = hashlib.sha256()

    try:
        with temp_path.open("wb") as handle:
            digest.update(content)
            handle.write(content)

        os.replace(temp_path, path)
    except Exception:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise

    return len(content), digest.hexdigest()


class StateStore:
    def __init__(self, email_root: Path) -> None:
        self.email_root = email_root
        self.path = email_root / "_state" / MESSAGE_INDEX_DB_FILENAME
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.configure()
        self.initialize_schema()

    def configure(self) -> None:
        for statement in (
            "PRAGMA foreign_keys = ON",
            "PRAGMA journal_mode = WAL",
            "PRAGMA synchronous = NORMAL",
            "PRAGMA temp_store = MEMORY",
            "PRAGMA busy_timeout = 5000",
        ):
            self.connection.execute(statement)

    def initialize_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                gmail_msg_id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                path TEXT NOT NULL,
                internal_date TEXT NOT NULL,
                byte_size INTEGER,
                sha256 TEXT,
                gmail_rfc822_size INTEGER,
                downloaded_at TEXT,
                last_seen_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_messages_path
                ON messages(path);

            CREATE TABLE IF NOT EXISTS message_labels (
                gmail_msg_id TEXT NOT NULL,
                label TEXT NOT NULL,
                PRIMARY KEY (gmail_msg_id, label),
                FOREIGN KEY (gmail_msg_id) REFERENCES messages(gmail_msg_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_message_labels_label
                ON message_labels(label);

            CREATE TABLE IF NOT EXISTS mailboxes (
                mailbox_name TEXT PRIMARY KEY,
                uidvalidity TEXT,
                last_seen_uid INTEGER,
                backfill_before_uid INTEGER,
                backfill_complete INTEGER NOT NULL DEFAULT 0,
                scan_highest_uid INTEGER,
                last_completed_at TEXT
            );
            """
        )
        self.connection.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
            ("schema_version", "2"),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def commit(self) -> None:
        self.connection.commit()

    def entry_from_row(self, row: sqlite3.Row, labels: list[str]) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "gmail_msg_id": row["gmail_msg_id"],
            "filename": row["filename"],
            "path": row["path"],
            "internal_date": row["internal_date"],
            "labels": sorted(labels),
            "last_seen_at": row["last_seen_at"],
        }

        for db_key, entry_key in (
            ("byte_size", "byte_size"),
            ("sha256", "sha256"),
            ("gmail_rfc822_size", "gmail_rfc822_size"),
            ("downloaded_at", "downloaded_at"),
        ):
            value = row[db_key]
            if value is not None:
                entry[entry_key] = value

        return entry

    def get_messages(self, gmail_msg_ids: list[str]) -> dict[str, dict[str, Any]]:
        ids = sorted({gmail_msg_id for gmail_msg_id in gmail_msg_ids if gmail_msg_id})
        if not ids:
            return {}

        rows_by_id: dict[str, sqlite3.Row] = {}
        labels_by_id: dict[str, list[str]] = {gmail_msg_id: [] for gmail_msg_id in ids}

        for offset in range(0, len(ids), SQLITE_QUERY_CHUNK_SIZE):
            chunk = ids[offset : offset + SQLITE_QUERY_CHUNK_SIZE]
            placeholders = ",".join("?" for _ in chunk)
            for row in self.connection.execute(
                f"SELECT * FROM messages WHERE gmail_msg_id IN ({placeholders})",
                chunk,
            ):
                rows_by_id[row["gmail_msg_id"]] = row

            for row in self.connection.execute(
                f"""
                SELECT gmail_msg_id, label
                FROM message_labels
                WHERE gmail_msg_id IN ({placeholders})
                ORDER BY label
                """,
                chunk,
            ):
                labels_by_id.setdefault(row["gmail_msg_id"], []).append(row["label"])

        return {
            gmail_msg_id: self.entry_from_row(row, labels_by_id.get(gmail_msg_id, []))
            for gmail_msg_id, row in rows_by_id.items()
        }

    def upsert_message(self, entry: dict[str, Any]) -> None:
        gmail_msg_id = str(entry["gmail_msg_id"])
        labels = sorted({label for label in entry.get("labels", []) if isinstance(label, str)})
        self.connection.execute(
            """
            INSERT INTO messages (
                gmail_msg_id,
                filename,
                path,
                internal_date,
                byte_size,
                sha256,
                gmail_rfc822_size,
                downloaded_at,
                last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(gmail_msg_id) DO UPDATE SET
                filename = excluded.filename,
                path = excluded.path,
                internal_date = excluded.internal_date,
                byte_size = excluded.byte_size,
                sha256 = excluded.sha256,
                gmail_rfc822_size = excluded.gmail_rfc822_size,
                downloaded_at = COALESCE(excluded.downloaded_at, messages.downloaded_at),
                last_seen_at = excluded.last_seen_at
            """,
            (
                gmail_msg_id,
                str(entry["filename"]),
                str(entry["path"]),
                str(entry["internal_date"]),
                entry.get("byte_size"),
                entry.get("sha256"),
                entry.get("gmail_rfc822_size"),
                entry.get("downloaded_at"),
                entry.get("last_seen_at") or iso_now(),
            ),
        )

        self.connection.executemany(
            "INSERT OR IGNORE INTO message_labels(gmail_msg_id, label) VALUES (?, ?)",
            ((gmail_msg_id, label) for label in labels),
        )

    def mark_seen(self, gmail_msg_id: str, label: str) -> bool:
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO message_labels(gmail_msg_id, label) VALUES (?, ?)",
            (gmail_msg_id, label),
        )
        self.connection.execute(
            "UPDATE messages SET last_seen_at = ? WHERE gmail_msg_id = ?",
            (iso_now(), gmail_msg_id),
        )
        return cursor.rowcount > 0

    def get_mailbox_state(self, mailbox_name: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM mailboxes WHERE mailbox_name = ?",
            (mailbox_name,),
        ).fetchone()
        if row is None:
            return {}

        return {
            "uidvalidity": row["uidvalidity"],
            "last_seen_uid": row["last_seen_uid"],
            "backfill_before_uid": row["backfill_before_uid"],
            "backfill_complete": bool(row["backfill_complete"]),
            "scan_highest_uid": row["scan_highest_uid"],
            "last_completed_at": row["last_completed_at"],
        }

    def update_mailbox_state(self, mailbox_name: str, **updates: Any) -> None:
        state = self.get_mailbox_state(mailbox_name)
        state.update(updates)
        self.connection.execute(
            """
            INSERT INTO mailboxes (
                mailbox_name,
                uidvalidity,
                last_seen_uid,
                backfill_before_uid,
                backfill_complete,
                scan_highest_uid,
                last_completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mailbox_name) DO UPDATE SET
                uidvalidity = excluded.uidvalidity,
                last_seen_uid = excluded.last_seen_uid,
                backfill_before_uid = excluded.backfill_before_uid,
                backfill_complete = excluded.backfill_complete,
                scan_highest_uid = excluded.scan_highest_uid,
                last_completed_at = excluded.last_completed_at
            """,
            (
                mailbox_name,
                state.get("uidvalidity"),
                int_from_state(state.get("last_seen_uid")),
                int_from_state(state.get("backfill_before_uid")),
                1 if state.get("backfill_complete") else 0,
                int_from_state(state.get("scan_highest_uid")),
                state.get("last_completed_at"),
            ),
        )

    def has_missing_files(self) -> bool:
        for row in self.connection.execute("SELECT path, filename FROM messages"):
            entry = {"path": row["path"], "filename": row["filename"]}
            if message_file_exists(self.email_root, entry) is None:
                return True
        return False

    def count_messages(self) -> int:
        row = self.connection.execute("SELECT COUNT(*) AS count FROM messages").fetchone()
        return int(row["count"]) if row is not None else 0


def write_sync_state(email_root: Path, summary: dict[str, Any]) -> None:
    write_json_atomic(email_root / "_state" / "sync_state.json", summary)


def selected_uidvalidity(connection: imaplib.IMAP4_SSL) -> str | None:
    status, data = connection.response("UIDVALIDITY")
    if status != "OK" or not data:
        return None

    for item in data:
        if not isinstance(item, bytes):
            continue
        match = re.search(rb"(\d+)", item)
        if match:
            return match.group(1).decode("ascii")
    return None


def select_mailbox(connection: imaplib.IMAP4_SSL, mailbox: Mailbox) -> tuple[int, str | None]:
    status, data = connection.select(mailbox.select_arg, readonly=True)
    if status != "OK":
        raise RuntimeError(f"Could not select mailbox {mailbox.display_name!r}: {data!r}")
    uidvalidity = selected_uidvalidity(connection)
    if data and data[0] is not None:
        try:
            return int(data[0]), uidvalidity
        except (TypeError, ValueError):
            return 0, uidvalidity
    return 0, uidvalidity


def int_from_state(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def int_from_stats(stats: dict[str, int], key: str, default: int) -> int:
    value = stats.get(key, default)
    return value if isinstance(value, int) and value > 0 else default


def max_uid_value(uids: list[bytes]) -> int:
    return max(int(uid) for uid in uids)


def min_uid_value(uids: list[bytes]) -> int:
    return min(int(uid) for uid in uids)


def restore_existing_labels(replacement: dict[str, Any], previous_entry: dict[str, Any] | None) -> None:
    if previous_entry is None:
        return

    labels = previous_entry.get("labels", [])
    if not isinstance(labels, list):
        return

    for label in labels:
        if isinstance(label, str):
            merge_label(replacement, label)


def format_exception(exc: BaseException) -> str:
    class_name = exc.__class__.__name__
    if isinstance(exc, imaplib.IMAP4.abort):
        class_name = "IMAP abort"

    detail = str(exc).strip()
    return f"{class_name}: {detail}" if detail else class_name


def is_retryable_download_error(error: str | None) -> bool:
    if not error:
        return False
    normalized = error.lower()
    return any(pattern in normalized for pattern in RETRYABLE_GMAIL_ERROR_PATTERNS) or any(
        pattern in normalized for pattern in RETRYABLE_NETWORK_ERROR_PATTERNS
    )


def worker_retry_delay(attempt_index: int) -> float:
    return min(WORKER_RETRY_MAX_DELAY_SECONDS, WORKER_RETRY_BASE_DELAY_SECONDS * (2 ** attempt_index))


def reduced_worker_count(current_count: int) -> int:
    if current_count > 8:
        return 8
    if current_count > 4:
        return 4
    if current_count > 2:
        return 2
    if current_count > 1:
        return 1
    return 1


def clamp_metadata_batch_size(batch_size: int) -> int:
    return max(MIN_METADATA_BATCH_SIZE, min(MAX_METADATA_BATCH_SIZE, batch_size))


def reduced_metadata_batch_size(current_size: int) -> int:
    if current_size > 500:
        return 500
    if current_size > 250:
        return 250
    if current_size > MIN_METADATA_BATCH_SIZE:
        return MIN_METADATA_BATCH_SIZE
    return MIN_METADATA_BATCH_SIZE


def increased_metadata_batch_size(current_size: int) -> int:
    if current_size < 250:
        return 250
    if current_size < 500:
        return 500
    if current_size < MAX_METADATA_BATCH_SIZE:
        return MAX_METADATA_BATCH_SIZE
    return MAX_METADATA_BATCH_SIZE


def sorted_pending_downloads(pending_items: list[PendingDownload]) -> list[PendingDownload]:
    return sorted(
        pending_items,
        key=lambda item: (item.metadata.rfc822_size or 0, int(item.uid)),
        reverse=True,
    )


def open_worker_connection(credentials: Credentials, mailbox: Mailbox) -> imaplib.IMAP4_SSL:
    connection = connect_and_login(credentials)
    try:
        select_mailbox(connection, mailbox)
    except Exception:
        close_connection(connection)
        raise
    return connection


class MetadataPrefetcher:
    def __init__(self, credentials: Credentials, mailbox: Mailbox) -> None:
        self.credentials = credentials
        self.mailbox = mailbox
        self.connection: imaplib.IMAP4_SSL | None = None
        self.task_queue: Queue[tuple[list[bytes], Future] | None] = Queue()
        self.closed = False
        self.lock = RLock()
        self.thread = Thread(target=self.run, daemon=True)
        self.thread.start()

    def submit(self, uids: list[bytes]) -> Future:
        future: Future = Future()
        with self.lock:
            if self.closed:
                future.set_exception(RuntimeError("metadata prefetcher is closed"))
                return future
            self.task_queue.put((list(uids), future))
        return future

    def run(self) -> None:
        try:
            while True:
                job = self.task_queue.get()
                try:
                    if job is None:
                        return

                    uids, future = job
                    if self.closed:
                        future.cancel()
                        continue
                    if not future.set_running_or_notify_cancel():
                        continue

                    try:
                        if self.connection is None:
                            self.connection = open_worker_connection(self.credentials, self.mailbox)
                        future.set_result(fetch_metadata_batch(self.connection, uids))
                    except BaseException as exc:
                        close_connection(self.connection)
                        self.connection = None
                        future.set_exception(exc)
                finally:
                    self.task_queue.task_done()
        finally:
            close_connection(self.connection)
            self.connection = None

    def close(self, *, wait: bool = False, timeout: float = 1.0) -> None:
        with self.lock:
            if self.closed:
                return
            self.closed = True
            self.task_queue.put(None)

        close_connection(self.connection)
        if wait and self.thread.is_alive():
            self.thread.join(timeout=timeout)

    def __del__(self) -> None:
        try:
            self.close(wait=False)
        except Exception:
            pass


def schedule_metadata_batch(
    prefetcher: MetadataPrefetcher,
    offset: int,
    uids: list[bytes],
    batch_size: int,
) -> MetadataBatchJob:
    uid_batch = uids[offset : offset + batch_size]
    return MetadataBatchJob(
        offset=offset,
        batch_size=batch_size,
        uids=uid_batch,
        future=prefetcher.submit(uid_batch),
    )


def download_pending_with_retries(
    credentials: Credentials,
    mailbox: Mailbox,
    pending: PendingDownload,
    connection: imaplib.IMAP4_SSL | None,
) -> tuple[imaplib.IMAP4_SSL | None, DownloadOutcome]:
    retries = 0
    reconnects = 0

    for attempt_index in range(WORKER_MESSAGE_ATTEMPTS):
        try:
            if connection is None:
                connection = open_worker_connection(credentials, mailbox)

            raw_message = fetch_raw_message(connection, pending.uid)
            byte_size, digest = write_raw_message(pending.final_path, raw_message)
            if pending.metadata.rfc822_size is not None and byte_size != pending.metadata.rfc822_size:
                try:
                    pending.final_path.unlink(missing_ok=True)
                except OSError:
                    pass
                raise RuntimeError(
                    "downloaded size mismatch for UID "
                    f"{pending.uid.decode('ascii', 'replace')}: got {byte_size} bytes, "
                    f"expected {pending.metadata.rfc822_size}"
                )
            return (
                connection,
                DownloadOutcome(
                    pending=pending,
                    byte_size=byte_size,
                    sha256=digest,
                    downloaded_at=iso_now(),
                    error=None,
                    retries=retries,
                    reconnects=reconnects,
                ),
            )
        except Exception as exc:
            error = format_exception(exc)
            if not is_retryable_download_error(error):
                return (
                    connection,
                    DownloadOutcome(
                        pending=pending,
                        byte_size=None,
                        sha256=None,
                        downloaded_at=None,
                        error=error,
                        retries=retries,
                        reconnects=reconnects,
                    ),
                )

            close_connection(connection)
            connection = None
            if attempt_index >= WORKER_MESSAGE_ATTEMPTS - 1:
                return (
                    connection,
                    DownloadOutcome(
                        pending=pending,
                        byte_size=None,
                        sha256=None,
                        downloaded_at=None,
                        error=f"{error} (after {WORKER_MESSAGE_ATTEMPTS} attempts)",
                        retries=retries,
                        reconnects=reconnects,
                    ),
                )

            retries += 1
            reconnects += 1
            time.sleep(worker_retry_delay(attempt_index))

    return (
        connection,
        DownloadOutcome(
            pending=pending,
            byte_size=None,
            sha256=None,
            downloaded_at=None,
            error="download worker exhausted retry attempts",
            retries=retries,
            reconnects=reconnects,
        ),
    )


def download_worker(
    credentials: Credentials,
    mailbox: Mailbox,
    task_queue: Queue[PendingDownload | None],
    result_queue: Queue[DownloadOutcome],
) -> None:
    connection: imaplib.IMAP4_SSL | None = None
    connection_error: str | None = None

    try:
        connection = open_worker_connection(credentials, mailbox)
    except Exception as exc:
        connection_error = format_exception(exc)
        if is_retryable_download_error(connection_error):
            connection_error = None

    try:
        while True:
            pending = task_queue.get()
            try:
                if pending is None:
                    return

                if connection_error is not None:
                    result_queue.put(
                        DownloadOutcome(
                            pending=pending,
                            byte_size=None,
                            sha256=None,
                            downloaded_at=None,
                            error=connection_error,
                        )
                    )
                    continue

                connection, outcome = download_pending_with_retries(credentials, mailbox, pending, connection)
                result_queue.put(outcome)
            except Exception as exc:
                if pending is not None:
                    result_queue.put(
                        DownloadOutcome(
                            pending=pending,
                            byte_size=None,
                            sha256=None,
                            downloaded_at=None,
                            error=format_exception(exc),
                        )
                    )
            finally:
                task_queue.task_done()
    finally:
        close_connection(connection)


class DownloadWorkerPool:
    def __init__(self, credentials: Credentials, mailbox: Mailbox, worker_count: int) -> None:
        self.credentials = credentials
        self.mailbox = mailbox
        self.worker_count = max(1, worker_count)
        self.task_queue: Queue[PendingDownload | None] = Queue()
        self.result_queue: Queue[DownloadOutcome] = Queue()
        self.closed = False
        self.threads = [
            Thread(
                target=download_worker,
                args=(credentials, mailbox, self.task_queue, self.result_queue),
                daemon=True,
            )
            for _ in range(self.worker_count)
        ]
        for thread in self.threads:
            thread.start()

    def download(
        self,
        pending_items: list[PendingDownload],
        progress_callback: Callable[[int, int, int, int, int, int, int, int, int], None] | None = None,
    ) -> list[DownloadOutcome]:
        if self.closed:
            raise RuntimeError("download worker pool is closed")
        if not pending_items:
            return []

        pending_items = sorted_pending_downloads(pending_items)
        expected_bytes = sum(item.metadata.rfc822_size or 0 for item in pending_items)
        outcomes: list[DownloadOutcome] = []
        successful_downloads = 0
        newly_archived_downloads = 0
        downloaded_bytes = 0
        download_retries = 0
        worker_reconnects = 0

        for pending in pending_items:
            self.task_queue.put(pending)

        if progress_callback is not None:
            progress_callback(
                0,
                len(pending_items),
                0,
                0,
                0,
                expected_bytes,
                download_retries,
                worker_reconnects,
                self.worker_count,
            )

        while len(outcomes) < len(pending_items):
            outcome = self.result_queue.get()
            outcomes.append(outcome)
            if outcome.byte_size is not None:
                downloaded_bytes += outcome.byte_size
                successful_downloads += 1
                if not outcome.pending.restoring:
                    newly_archived_downloads += 1
            download_retries += outcome.retries
            worker_reconnects += outcome.reconnects
            completed = len(outcomes)
            if progress_callback is not None:
                progress_callback(
                    completed,
                    len(pending_items),
                    successful_downloads,
                    newly_archived_downloads,
                    downloaded_bytes,
                    expected_bytes,
                    download_retries,
                    worker_reconnects,
                    self.worker_count,
                )

        self.task_queue.join()
        return outcomes

    def close(self, *, wait: bool = False, timeout: float = 1.0) -> None:
        if self.closed:
            return

        self.closed = True
        for _ in self.threads:
            self.task_queue.put(None)
        if wait:
            deadline = time.monotonic() + timeout
            for thread in self.threads:
                remaining = max(0.0, deadline - time.monotonic())
                thread.join(timeout=remaining)


class DownloadPoolManager:
    def __init__(self, credentials: Credentials, mailbox: Mailbox) -> None:
        self.credentials = credentials
        self.mailbox = mailbox
        self.pool: DownloadWorkerPool | None = None

    def download(
        self,
        worker_count: int,
        pending_items: list[PendingDownload],
        progress_callback: Callable[[int, int, int, int, int, int, int, int, int], None] | None = None,
    ) -> list[DownloadOutcome]:
        if self.pool is None or self.pool.worker_count != worker_count:
            self.close()
            self.pool = DownloadWorkerPool(self.credentials, self.mailbox, worker_count)
        return self.pool.download(pending_items, progress_callback=progress_callback)

    def close(self, *, wait: bool = False) -> None:
        if self.pool is None:
            return
        self.pool.close(wait=wait)
        self.pool = None


def download_pending_messages(
    credentials: Credentials,
    mailbox: Mailbox,
    pending_items: list[PendingDownload],
    worker_count: int,
    progress_callback: Callable[[int, int, int, int, int, int, int, int, int], None] | None = None,
) -> list[DownloadOutcome]:
    if not pending_items:
        return []

    worker_count = max(1, min(worker_count, len(pending_items)))
    pool = DownloadWorkerPool(credentials, mailbox, worker_count)
    interrupted = False
    try:
        return pool.download(pending_items, progress_callback=progress_callback)
    except KeyboardInterrupt:
        interrupted = True
        raise
    finally:
        pool.close(wait=not interrupted)


def download_with_adaptive_workers(
    credentials: Credentials,
    mailbox: Mailbox,
    pending_items: list[PendingDownload],
    status: LiveStatus,
    stats: dict[str, Any],
    download_manager: DownloadPoolManager | None = None,
    progress_callback: Callable[[str, bool], None] | None = None,
) -> list[DownloadOutcome]:
    remaining = pending_items
    completed: list[DownloadOutcome] = []
    active_successful_downloads = 0
    active_newly_archived = 0
    active_downloaded_bytes = 0

    while remaining:
        worker_count = int_from_stats(stats, "download_workers", DEFAULT_DOWNLOAD_WORKERS)

        def on_download_progress(
            completed: int,
            total: int,
            successful_downloads: int,
            newly_archived_downloads: int,
            downloaded_bytes: int,
            expected_bytes: int,
            download_retries: int,
            worker_reconnects: int,
            _active_worker_count: int,
        ) -> None:
            stats["active_downloaded"] = active_successful_downloads + successful_downloads
            stats["active_archived"] = active_newly_archived + newly_archived_downloads
            stats["active_downloaded_bytes"] = active_downloaded_bytes + downloaded_bytes
            if progress_callback is not None:
                progress_callback(
                    download_activity(
                        completed,
                        total,
                        downloaded_bytes,
                        expected_bytes,
                        download_retries,
                        worker_reconnects,
                    ),
                    completed == total,
                )

        if download_manager is None:
            outcomes = download_pending_messages(
                credentials,
                mailbox,
                remaining,
                worker_count,
                progress_callback=on_download_progress,
            )
        else:
            outcomes = download_manager.download(
                worker_count,
                remaining,
                progress_callback=on_download_progress,
            )
        for outcome in outcomes:
            if outcome.byte_size is None:
                continue
            active_successful_downloads += 1
            active_downloaded_bytes += outcome.byte_size
            if not outcome.pending.restoring:
                active_newly_archived += 1
        stats["download_retries"] += sum(outcome.retries for outcome in outcomes)
        stats["worker_reconnects"] += sum(outcome.reconnects for outcome in outcomes)
        retryable = [
            outcome
            for outcome in outcomes
            if outcome.error is not None and is_retryable_download_error(outcome.error)
        ]
        final = [
            outcome
            for outcome in outcomes
            if outcome.error is None or not is_retryable_download_error(outcome.error)
        ]
        completed.extend(final)

        if not retryable:
            break

        next_worker_count = reduced_worker_count(worker_count)
        if next_worker_count == worker_count:
            completed.extend(retryable)
            break

        stats["download_workers"] = next_worker_count
        stats["worker_backoffs"] += 1
        if download_manager is not None:
            download_manager.close()
        remaining = [outcome.pending for outcome in retryable]
        status.line(
            f"Gmail throttled or dropped connections; retrying {len(remaining)} messages "
            f"with {next_worker_count} workers."
        )

    return completed


def mark_work_done(stats: dict[str, Any], count: int = 1) -> None:
    if count <= 0:
        return
    total = stats.get("work_total", 0)
    next_value = stats.get("work_done", 0) + count
    stats["work_done"] = min(next_value, total) if total > 0 else next_value


def reset_eta_sample(stats: dict[str, Any]) -> None:
    stats["eta_started_at"] = datetime.now().timestamp()
    stats["eta_baseline_done"] = stats.get("work_done", 0) + stats.get("active_archived", 0)
    stats["eta_smoothed_remaining"] = None


def smoothed_eta(stats: dict[str, Any], raw_eta: float | None) -> float | None:
    if raw_eta is None:
        return None

    previous = stats.get("eta_smoothed_remaining")
    if isinstance(previous, (int, float)) and previous > 0 and raw_eta > 0:
        raw_eta = (ETA_SMOOTHING_ALPHA * raw_eta) + ((1 - ETA_SMOOTHING_ALPHA) * float(previous))

    stats["eta_smoothed_remaining"] = raw_eta
    return raw_eta


def recent_transfer_rate(stats: dict[str, Any], downloaded_bytes: int, now: float) -> float | None:
    last_at = stats.get("transfer_rate_last_at")
    last_bytes = stats.get("transfer_rate_last_bytes")
    if not isinstance(last_at, (int, float)) or not isinstance(last_bytes, int) or last_at <= 0:
        stats["transfer_rate_last_at"] = now
        stats["transfer_rate_last_bytes"] = downloaded_bytes
        stats["transfer_rate_last_progress_at"] = now if downloaded_bytes > 0 else 0.0
        return None

    elapsed = max(0.001, now - float(last_at))
    byte_delta = downloaded_bytes - last_bytes
    if byte_delta > 0:
        instant_rate = byte_delta / elapsed
        previous_rate = stats.get("transfer_rate_bytes_per_second")
        if isinstance(previous_rate, (int, float)) and previous_rate > 0:
            instant_rate = (
                TRANSFER_RATE_SMOOTHING_ALPHA * instant_rate
                + (1 - TRANSFER_RATE_SMOOTHING_ALPHA) * float(previous_rate)
            )
        stats["transfer_rate_bytes_per_second"] = instant_rate
        stats["transfer_rate_last_at"] = now
        stats["transfer_rate_last_bytes"] = downloaded_bytes
        stats["transfer_rate_last_progress_at"] = now
        return instant_rate

    last_progress_at = stats.get("transfer_rate_last_progress_at", 0.0)
    if isinstance(last_progress_at, (int, float)) and last_progress_at > 0:
        if now - float(last_progress_at) < TRANSFER_RATE_IDLE_SECONDS:
            rate = stats.get("transfer_rate_bytes_per_second")
            return float(rate) if isinstance(rate, (int, float)) and rate > 0 else None

    stats["transfer_rate_bytes_per_second"] = 0.0
    stats["transfer_rate_last_at"] = now
    stats["transfer_rate_last_bytes"] = downloaded_bytes
    return 0.0 if downloaded_bytes > 0 else None


def format_transfer_rate(rate: float | None) -> str:
    if rate is None:
        return "Transfer waiting for raw email data"
    if rate <= 0:
        return "Transfer idle"
    return f"Transfer {format_bytes(int(rate))}/s"


def total_progress_message(stats: dict[str, Any], started_at: float, activity: str = "") -> str:
    now = datetime.now().timestamp()
    active_archived = stats.get("active_archived", 0)
    done = stats.get("work_done", 0) + active_archived
    total = stats.get("work_total", 0)
    if total > 0:
        done = min(done, total)
    elapsed = max(0.001, now - started_at)
    eta_started_at = stats.get("eta_started_at", 0)
    eta_baseline_done = stats.get("eta_baseline_done", done)
    sample_elapsed = max(0.001, now - eta_started_at) if isinstance(eta_started_at, (int, float)) and eta_started_at > 0 else 0
    sample_done = max(0, done - eta_baseline_done) if isinstance(eta_baseline_done, int) else 0
    eta = smoothed_eta(stats, estimate_remaining(done, total, sample_done, sample_elapsed))
    downloaded_bytes = stats["downloaded_bytes"] + stats.get("active_downloaded_bytes", 0)
    transfer_rate = recent_transfer_rate(stats, downloaded_bytes, now)
    downloaded = (
        stats["downloaded"]
        + stats["restored_missing"]
        + stats["repaired_corrupt"]
        + stats.get("active_downloaded", 0)
    )
    current = activity or "checking for missing emails..."

    return "\n".join(
        [
            "Gmail Downloader",
            f"{progress_bar(done, total)} {progress_percent(done, total).strip()}  "
            f"{format_int(done)} / {format_int(total)} emails archived",
            f"Downloaded {format_int(downloaded)} emails, {format_bytes(downloaded_bytes)}",
            f"{format_transfer_rate(transfer_rate)}    Elapsed {format_duration(elapsed)}    "
            f"Left {format_remaining(eta)}",
            f"Current: {current}",
        ]
    )


class ProgressDisplay:
    def __init__(self, status: LiveStatus, stats: dict[str, Any], started_at: float) -> None:
        self.status = status
        self.stats = stats
        self.started_at = started_at
        self.activity = "checking for missing emails..."
        self.last_message = ""
        self.stop_event = Event()
        self.thread: Thread | None = None
        self.lock = RLock()

    def start(self) -> None:
        if not self.status.dynamic or self.thread is not None:
            return

        self.thread = Thread(target=self.run, daemon=True)
        self.thread.start()

    def update(self, activity: str | None = None, *, force: bool = False) -> None:
        with self.lock:
            if activity is not None:
                self.activity = activity
            self.render_locked(force=force)

    def render_locked(self, *, force: bool = False) -> None:
        message = total_progress_message(self.stats, self.started_at, self.activity)
        if not force and message == self.last_message:
            return

        self.last_message = message
        self.status.update(message, force=force)

    def run(self) -> None:
        while not self.stop_event.wait(STATUS_HEARTBEAT_SECONDS):
            with self.lock:
                self.render_locked(force=False)

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=1.0)


def initial_stats() -> dict[str, Any]:
    return {
        "downloaded": 0,
        "downloaded_bytes": 0,
        "skipped": 0,
        "restored_missing": 0,
        "repaired_corrupt": 0,
        "corrupt_found": 0,
        "indexed_existing": 0,
        "labels_merged": 0,
        "errors": 0,
        "download_workers": DEFAULT_DOWNLOAD_WORKERS,
        "worker_backoffs": 0,
        "download_retries": 0,
        "worker_reconnects": 0,
        "metadata_batch_size": DEFAULT_METADATA_BATCH_SIZE,
        "metadata_batch_backoffs": 0,
        "metadata_batch_growths": 0,
        "metadata_batch_successes": 0,
        "work_done": 0,
        "work_total": 0,
        "account_message_total": 0,
        "active_downloaded": 0,
        "active_downloaded_bytes": 0,
        "active_archived": 0,
        "eta_started_at": 0.0,
        "eta_baseline_done": 0,
        "eta_smoothed_remaining": None,
        "transfer_rate_last_at": 0.0,
        "transfer_rate_last_bytes": 0,
        "transfer_rate_last_progress_at": 0.0,
        "transfer_rate_bytes_per_second": None,
    }


def download_activity(
    completed: int,
    total: int,
    downloaded_bytes: int,
    expected_bytes: int,
    _download_retries: int,
    _worker_reconnects: int,
) -> str:
    byte_progress = format_bytes(downloaded_bytes)
    if expected_bytes > 0:
        byte_progress += f"/{format_bytes(expected_bytes)}"

    return f"downloading {format_int(completed)} of {format_int(total)} in active batch ({byte_progress})"


def print_summary_section(title: str, rows: list[tuple[str, str]]) -> None:
    print(title)
    label_width = max((len(label) for label, _value in rows), default=0)
    for label, value in rows:
        print(f"  {label.ljust(label_width)}  {value}")


def print_final_summary(summary: dict[str, Any], email_root: Path, elapsed_seconds: float) -> None:
    raw_downloaded = summary["downloaded"] + summary["restored_missing"] + summary["repaired_corrupt"]
    byte_rate = int(summary["downloaded_bytes"] / max(elapsed_seconds, 0.001))
    status = "success" if summary["errors"] == 0 else "completed with errors"

    print()
    print(f"Done in {format_duration(elapsed_seconds)} ({status}).")
    print()
    print_summary_section(
        "Messages",
        [
            ("archived coverage", f"{format_int(summary['work_done'])}/{format_int(summary['work_total'])}"),
            ("account messages", format_int(summary["account_message_total"])),
            ("downloaded", format_int(summary["downloaded"])),
            ("restored missing", format_int(summary["restored_missing"])),
            ("repaired corrupt", format_int(summary["repaired_corrupt"])),
            ("indexed existing", format_int(summary["indexed_existing"])),
            ("skipped", format_int(summary["skipped"])),
            ("total indexed", format_int(summary["total_messages_indexed"])),
        ],
    )
    print()
    print_summary_section(
        "Transfer",
        [
            ("raw downloaded", format_int(raw_downloaded)),
            ("downloaded bytes", format_bytes(summary["downloaded_bytes"])),
            ("average speed", f"{format_bytes(byte_rate)}/s"),
            ("raw emails", str(email_root / "raw")),
        ],
    )
    print()
    print_summary_section(
        "Reliability",
        [
            ("errors", format_int(summary["errors"])),
            ("corrupt files found", format_int(summary["corrupt_found"])),
            ("download retries", format_int(summary["download_retries"])),
            ("worker reconnects", format_int(summary["worker_reconnects"])),
        ]
        + (
            [("partial files cleaned", format_int(summary["removed_part_files"]))]
            if summary["removed_part_files"]
            else []
        ),
    )
    print()
    print_summary_section(
        "Tuning",
        [
            ("final workers", format_int(summary["download_workers"])),
            ("worker backoffs", format_int(summary["worker_backoffs"])),
            ("metadata batch", format_int(summary["metadata_batch_size"])),
            ("metadata backoffs", format_int(summary["metadata_batch_backoffs"])),
            ("metadata growths", format_int(summary["metadata_batch_growths"])),
        ],
    )


def build_mailbox_plan_from_state(
    connection: imaplib.IMAP4_SSL,
    mailbox: Mailbox,
    state: dict[str, Any],
    *,
    force_full_scan: bool,
) -> MailboxPlan:
    message_count, uidvalidity = select_mailbox(connection, mailbox)
    last_seen_uid = int_from_state(state.get("last_seen_uid"))
    backfill_before_uid = int_from_state(state.get("backfill_before_uid"))
    same_uidvalidity = uidvalidity is not None and state.get("uidvalidity") == uidvalidity
    backfill_complete = bool(state.get("backfill_complete"))

    sync_mode = "full"
    start_uid: int | None = None
    end_uid: int | None = None

    if not force_full_scan and same_uidvalidity and backfill_complete and last_seen_uid is not None:
        sync_mode = "incremental"
        start_uid = last_seen_uid + 1
    elif not force_full_scan and same_uidvalidity and not backfill_complete and backfill_before_uid is not None:
        sync_mode = "backfill"
        end_uid = backfill_before_uid - 1

    return MailboxPlan(
        mailbox=mailbox,
        message_count=message_count,
        uidvalidity=uidvalidity,
        state=state,
        sync_mode=sync_mode,
        start_uid=start_uid,
        end_uid=end_uid,
        uids=search_uids(connection, start_uid=start_uid, end_uid=end_uid),
    )


def build_mailbox_plan_worker(
    credentials: Credentials,
    mailbox: Mailbox,
    state: dict[str, Any],
    force_full_scan: bool,
) -> MailboxPlan:
    connection = connect_and_login(credentials)
    try:
        return build_mailbox_plan_from_state(connection, mailbox, state, force_full_scan=force_full_scan)
    finally:
        close_connection(connection)


def build_mailbox_plan_batch(
    credentials: Credentials,
    mailboxes: list[Mailbox],
    mailbox_states: dict[str, dict[str, Any]],
    force_full_scan: bool,
    worker_count: int,
) -> list[tuple[Mailbox, MailboxPlan | None, str | None]]:
    if not mailboxes:
        return []

    task_queue: Queue[Mailbox | None] = Queue()
    result_queue: Queue[tuple[Mailbox, MailboxPlan | None, str | None]] = Queue()

    def worker() -> None:
        while True:
            mailbox = task_queue.get()
            try:
                if mailbox is None:
                    return

                try:
                    plan = build_mailbox_plan_worker(
                        credentials,
                        mailbox,
                        mailbox_states[mailbox.display_name],
                        force_full_scan,
                    )
                except Exception as exc:
                    result_queue.put((mailbox, None, format_exception(exc)))
                else:
                    result_queue.put((mailbox, plan, None))
            finally:
                task_queue.task_done()

    threads = [
        Thread(target=worker, daemon=True)
        for _ in range(max(1, min(worker_count, len(mailboxes))))
    ]
    for thread in threads:
        thread.start()
    for mailbox in mailboxes:
        task_queue.put(mailbox)
    for _ in threads:
        task_queue.put(None)

    results: list[tuple[Mailbox, MailboxPlan | None, str | None]] = []
    while len(results) < len(mailboxes):
        results.append(result_queue.get())

    for thread in threads:
        thread.join(timeout=1.0)
    return results


def plan_mailboxes_concurrently(
    credentials: Credentials,
    mailboxes: list[Mailbox],
    state_store: StateStore,
    force_full_scan: bool,
    indexed_message_count: int,
    stats: dict[str, Any],
    status: LiveStatus,
    progress: ProgressDisplay,
) -> list[MailboxPlan]:
    mailbox_states = {mailbox.display_name: state_store.get_mailbox_state(mailbox.display_name) for mailbox in mailboxes}
    worker_count = max(1, min(DEFAULT_DOWNLOAD_WORKERS, len(mailboxes)))
    remaining = mailboxes
    plans_by_name: dict[str, MailboxPlan] = {}

    progress.update("preparing account scan...", force=True)

    while remaining:
        retry_mailboxes: list[Mailbox] = []
        retry_errors: dict[str, str] = {}
        active_workers = max(1, min(worker_count, len(remaining)))

        for mailbox, plan, error in build_mailbox_plan_batch(
            credentials,
            remaining,
            mailbox_states,
            force_full_scan,
            active_workers,
        ):
            if error is not None:
                if is_retryable_download_error(error):
                    retry_mailboxes.append(mailbox)
                    retry_errors[mailbox.display_name] = error
                else:
                    status.line()
                    stats["errors"] += 1
                    print(f"Mailbox planning error: {mailbox.display_name}: {error}")
                continue

            if plan is None:
                continue

            plans_by_name[mailbox.display_name] = plan
            stats["account_message_total"] = max(stats["account_message_total"], plan.message_count)
            stats["work_total"] = stats["account_message_total"]
            stats["work_done"] = min(indexed_message_count, stats["work_total"])
            progress.update("preparing account scan...", force=False)

        if not retry_mailboxes:
            break

        next_worker_count = reduced_worker_count(active_workers)
        if next_worker_count == active_workers:
            for mailbox in retry_mailboxes:
                status.line()
                stats["errors"] += 1
                error = retry_errors.get(mailbox.display_name, "retryable Gmail connection limit reached")
                print(f"Mailbox planning error: {mailbox.display_name}: {error}")
            break

        worker_count = next_worker_count
        remaining = retry_mailboxes
        status.line(
            f"Gmail throttled mailbox planning; retrying {len(remaining)} mailboxes "
            f"with {worker_count} workers."
        )

    return [plans_by_name[mailbox.display_name] for mailbox in mailboxes if mailbox.display_name in plans_by_name]


def sync_mailbox(
    connection: imaplib.IMAP4_SSL,
    credentials: Credentials,
    plan: MailboxPlan,
    email_root: Path,
    existing_files: dict[str, Path],
    state_store: StateStore,
    stats: dict[str, Any],
    status: LiveStatus,
    progress: ProgressDisplay,
) -> None:
    mailbox = plan.mailbox
    raw_dir = email_root / "raw"
    _message_count, current_uidvalidity = select_mailbox(connection, mailbox)
    if plan.uidvalidity is not None and current_uidvalidity is not None and plan.uidvalidity != current_uidvalidity:
        raise RuntimeError(f"UIDVALIDITY changed for {mailbox.display_name}; rerun the downloader.")

    uidvalidity = current_uidvalidity or plan.uidvalidity
    state = dict(plan.state)
    last_seen_uid = int_from_state(state.get("last_seen_uid"))
    sync_mode = plan.sync_mode
    uids = plan.uids
    dirty_count = 0

    if not uids:
        updates: dict[str, Any] = {"last_completed_at": iso_now()}
        if uidvalidity is not None:
            updates["uidvalidity"] = uidvalidity
        if sync_mode in {"full", "backfill"}:
            scan_highest_uid = int_from_state(state.get("scan_highest_uid"))
            if scan_highest_uid is not None:
                updates["last_seen_uid"] = scan_highest_uid
            elif last_seen_uid is None:
                updates["last_seen_uid"] = 0
            updates["backfill_complete"] = True
            updates["backfill_before_uid"] = None
            updates["scan_highest_uid"] = None
        state_store.update_mailbox_state(mailbox.display_name, **updates)
        state_store.commit()
        return

    if sync_mode == "full":
        state["backfill_complete"] = False
        state["scan_highest_uid"] = max_uid_value(uids)
        state["backfill_before_uid"] = None

    progress.update("checking email details...", force=True)

    metadata_prefetcher = MetadataPrefetcher(credentials, mailbox)
    download_manager = DownloadPoolManager(credentials, mailbox)
    next_metadata_job: MetadataBatchJob | None = None
    offset = 0
    while offset < len(uids):
        if next_metadata_job is not None and next_metadata_job.offset == offset:
            metadata_job = next_metadata_job
            next_metadata_job = None
        else:
            current_batch_size = clamp_metadata_batch_size(
                int_from_stats(stats, "metadata_batch_size", DEFAULT_METADATA_BATCH_SIZE)
            )
            stats["metadata_batch_size"] = current_batch_size
            metadata_job = schedule_metadata_batch(metadata_prefetcher, offset, uids, current_batch_size)

        current_batch_size = metadata_job.batch_size
        uid_batch = metadata_job.uids
        batch_failed = False
        metadata_fallback = False
        pending_downloads: list[PendingDownload] = []
        newly_archived_without_download = 0

        progress.update(f"checking {format_int(len(uid_batch))} email details...", force=True)

        try:
            metadata_by_uid = metadata_job.future.result()
        except Exception as exc:
            next_batch_size = reduced_metadata_batch_size(current_batch_size)
            if next_batch_size < current_batch_size:
                stats["metadata_batch_size"] = next_batch_size
                stats["metadata_batch_backoffs"] += 1
                stats["metadata_batch_successes"] = 0
                status.line(
                    f"  Metadata batch of {len(uid_batch)} UIDs failed in {mailbox.display_name}; "
                    f"retrying with {next_batch_size}. {format_exception(exc)}"
                )
                continue

            metadata_fallback = True
            status.line(
                f"  Metadata batch failed at {current_batch_size} UIDs in {mailbox.display_name}; "
                f"fetching one UID at a time. {format_exception(exc)}"
            )
            metadata_by_uid = {}
            for uid in uid_batch:
                try:
                    metadata_by_uid[uid] = fetch_message_metadata(connection, uid)
                except Exception as item_exc:
                    stats["errors"] += 1
                    batch_failed = True
                    uid_text = uid.decode("ascii", "replace")
                    status.line(f"  Error on {mailbox.display_name} UID {uid_text}: {item_exc}")

        next_offset = offset + len(uid_batch)
        if not batch_failed and next_offset < len(uids):
            next_batch_size = clamp_metadata_batch_size(
                int_from_stats(stats, "metadata_batch_size", DEFAULT_METADATA_BATCH_SIZE)
            )
            next_metadata_job = schedule_metadata_batch(metadata_prefetcher, next_offset, uids, next_batch_size)

        messages_by_id = state_store.get_messages(
            [metadata.gmail_msg_id for metadata in metadata_by_uid.values()]
        )

        for uid in uid_batch:
            metadata = metadata_by_uid.get(uid)
            if metadata is None:
                continue

            entry = messages_by_id.get(metadata.gmail_msg_id)

            if isinstance(entry, dict):
                existing_path = message_file_exists(email_root, entry)
                if existing_path is not None:
                    if message_file_needs_repair(existing_path, metadata):
                        stats["corrupt_found"] += 1
                        pending_downloads.append(
                            PendingDownload(
                                uid=uid,
                                metadata=metadata,
                                final_path=existing_path,
                                mailbox_name=mailbox.display_name,
                                previous_entry=entry,
                                restoring=True,
                                repairing_corrupt=True,
                            )
                        )
                        continue

                    if state_store.mark_seen(metadata.gmail_msg_id, mailbox.display_name):
                        stats["labels_merged"] += 1
                    dirty_count += 1
                    stats["skipped"] += 1
                    continue

                existing_by_id = find_existing_message_file(existing_files, metadata.gmail_msg_id)
                if existing_by_id is not None:
                    if message_file_needs_repair(existing_by_id, metadata):
                        stats["corrupt_found"] += 1
                        pending_downloads.append(
                            PendingDownload(
                                uid=uid,
                                metadata=metadata,
                                final_path=existing_by_id,
                                mailbox_name=mailbox.display_name,
                                previous_entry=entry,
                                restoring=True,
                                repairing_corrupt=True,
                            )
                        )
                        continue

                    replacement = index_entry_for_file(
                        metadata,
                        existing_by_id,
                        email_root,
                        mailbox.display_name,
                        downloaded_at=entry.get("downloaded_at"),
                        sha256=entry.get("sha256") if isinstance(entry.get("sha256"), str) else None,
                    )
                    restore_existing_labels(replacement, entry)
                    state_store.upsert_message(replacement)
                    messages_by_id[metadata.gmail_msg_id] = replacement
                    existing_files[metadata.gmail_msg_id] = existing_by_id
                    stats["indexed_existing"] += 1
                    dirty_count += 1
                    continue

                final_path = raw_dir / filename_for_message(metadata)
                pending_downloads.append(
                    PendingDownload(
                        uid=uid,
                        metadata=metadata,
                        final_path=final_path,
                        mailbox_name=mailbox.display_name,
                        previous_entry=entry,
                        restoring=True,
                    )
                )
                continue
            else:
                existing_by_id = find_existing_message_file(existing_files, metadata.gmail_msg_id)
                if existing_by_id is not None:
                    if message_file_needs_repair(existing_by_id, metadata):
                        stats["corrupt_found"] += 1
                        pending_downloads.append(
                            PendingDownload(
                                uid=uid,
                                metadata=metadata,
                                final_path=existing_by_id,
                                mailbox_name=mailbox.display_name,
                                previous_entry=None,
                                restoring=False,
                                repairing_corrupt=True,
                            )
                        )
                        continue

                    entry = index_entry_for_file(
                        metadata,
                        existing_by_id,
                        email_root,
                        mailbox.display_name,
                        downloaded_at=None,
                    )
                    state_store.upsert_message(entry)
                    messages_by_id[metadata.gmail_msg_id] = entry
                    existing_files[metadata.gmail_msg_id] = existing_by_id
                    stats["indexed_existing"] += 1
                    dirty_count += 1
                    newly_archived_without_download += 1
                    continue

            filename = filename_for_message(metadata)
            final_path = raw_dir / filename

            if final_path.exists():
                if message_file_needs_repair(final_path, metadata):
                    stats["corrupt_found"] += 1
                    pending_downloads.append(
                        PendingDownload(
                            uid=uid,
                            metadata=metadata,
                            final_path=final_path,
                            mailbox_name=mailbox.display_name,
                            previous_entry=None,
                            restoring=False,
                            repairing_corrupt=True,
                        )
                    )
                    continue

                entry = index_entry_for_file(
                    metadata,
                    final_path,
                    email_root,
                    mailbox.display_name,
                    downloaded_at=None,
                )
                state_store.upsert_message(entry)
                messages_by_id[metadata.gmail_msg_id] = entry
                existing_files[metadata.gmail_msg_id] = final_path
                stats["indexed_existing"] += 1
                dirty_count += 1
                newly_archived_without_download += 1
                continue

            pending_downloads.append(
                PendingDownload(
                    uid=uid,
                    metadata=metadata,
                    final_path=final_path,
                    mailbox_name=mailbox.display_name,
                    previous_entry=None,
                    restoring=False,
                )
            )

        if newly_archived_without_download:
            mark_work_done(stats, newly_archived_without_download)
            progress.update("checking email details...", force=False)

        def on_download_progress(activity: str, force: bool = False) -> None:
            progress.update(activity, force=force)

        outcomes = download_with_adaptive_workers(
            credentials,
            mailbox,
            pending_downloads,
            status,
            stats,
            download_manager=download_manager,
            progress_callback=on_download_progress,
        )
        stats["active_downloaded"] = 0
        stats["active_downloaded_bytes"] = 0
        stats["active_archived"] = 0

        for outcome in outcomes:
            pending = outcome.pending
            if outcome.error is not None:
                stats["errors"] += 1
                batch_failed = True
                uid_text = pending.uid.decode("ascii", "replace")
                status.line(f"  Error on {mailbox.display_name} UID {uid_text}: {outcome.error}")
                continue

            if outcome.byte_size is None or outcome.sha256 is None or outcome.downloaded_at is None:
                stats["errors"] += 1
                batch_failed = True
                uid_text = pending.uid.decode("ascii", "replace")
                status.line(f"  Error on {mailbox.display_name} UID {uid_text}: incomplete download result")
                continue

            entry = index_entry_for_download(
                pending.metadata,
                pending.final_path,
                email_root,
                pending.mailbox_name,
                byte_size=outcome.byte_size,
                sha256=outcome.sha256,
                downloaded_at=outcome.downloaded_at,
            )
            restore_existing_labels(entry, pending.previous_entry)
            state_store.upsert_message(entry)
            messages_by_id[pending.metadata.gmail_msg_id] = entry
            existing_files[pending.metadata.gmail_msg_id] = pending.final_path
            stats["downloaded_bytes"] += outcome.byte_size
            if pending.repairing_corrupt:
                stats["repaired_corrupt"] += 1
                if not pending.restoring:
                    mark_work_done(stats)
            elif pending.restoring:
                stats["restored_missing"] += 1
            else:
                stats["downloaded"] += 1
                mark_work_done(stats)
            dirty_count += 1

        progress.update(force=True)

        if batch_failed:
            state_store.commit()
            status.line(
                f"  Stopped {mailbox.display_name} after a failed batch. "
                "Next run will resume from the last completed UID."
            )
            metadata_prefetcher.close()
            download_manager.close()
            return

        checkpoint: dict[str, Any] = {"last_completed_at": iso_now()}
        if uidvalidity is not None:
            checkpoint["uidvalidity"] = uidvalidity
        if sync_mode == "incremental":
            checkpoint["backfill_complete"] = True
        else:
            checkpoint["backfill_before_uid"] = min_uid_value(uid_batch)
            checkpoint["backfill_complete"] = False
            checkpoint["scan_highest_uid"] = state.get("scan_highest_uid")
        state_store.update_mailbox_state(mailbox.display_name, **checkpoint)
        dirty_count += 1
        offset += len(uid_batch)

        if metadata_fallback:
            stats["metadata_batch_successes"] = 0
        else:
            stats["metadata_batch_successes"] += 1
            if stats["metadata_batch_successes"] >= METADATA_BATCH_GROW_AFTER:
                next_batch_size = increased_metadata_batch_size(current_batch_size)
                if next_batch_size > current_batch_size:
                    stats["metadata_batch_size"] = next_batch_size
                    stats["metadata_batch_growths"] += 1
                stats["metadata_batch_successes"] = 0

        progress.update(force=True)

        if dirty_count >= DB_COMMIT_EVERY:
            state_store.commit()
            dirty_count = 0

    metadata_prefetcher.close()
    download_manager.close()
    if dirty_count:
        state_store.commit()
    if sync_mode == "incremental":
        final_updates = {
            "last_seen_uid": max(max_uid_value(uids), last_seen_uid or 0),
            "backfill_complete": True,
            "last_completed_at": iso_now(),
        }
        if uidvalidity is not None:
            final_updates["uidvalidity"] = uidvalidity
        state_store.update_mailbox_state(mailbox.display_name, **final_updates)
        state_store.commit()
    elif sync_mode in {"full", "backfill"}:
        scan_highest_uid = int_from_state(state.get("scan_highest_uid"))
        final_updates: dict[str, Any] = {
            "backfill_complete": True,
            "backfill_before_uid": None,
            "scan_highest_uid": None,
            "last_completed_at": iso_now(),
        }
        if scan_highest_uid is not None:
            final_updates["last_seen_uid"] = scan_highest_uid
        elif uids:
            final_updates["last_seen_uid"] = max_uid_value(uids)
        if uidvalidity is not None:
            final_updates["uidvalidity"] = uidvalidity
        state_store.update_mailbox_state(mailbox.display_name, **final_updates)
        state_store.commit()


def download_archive(credentials: Credentials, connection: imaplib.IMAP4_SSL, email_root: Path) -> dict[str, Any]:
    email_root.mkdir(parents=True, exist_ok=True)
    (email_root / "_state").mkdir(parents=True, exist_ok=True)
    (email_root / "raw").mkdir(parents=True, exist_ok=True)
    status = LiveStatus()
    operation_started_at = datetime.now().timestamp()
    stats = initial_stats()
    progress = ProgressDisplay(status, stats, operation_started_at)
    progress.update("preparing local archive...", force=True)
    progress.start()
    state_store: StateStore | None = None
    try:
        removed_part_files, existing_files = run_startup_file_tasks(email_root)
        state_store = StateStore(email_root)
        progress.update("preparing account scan...", force=True)

        mailboxes = list_selectable_mailboxes(connection)
        if not mailboxes:
            raise RuntimeError("No selectable Gmail mailboxes were found.")
        force_full_scan = state_store.has_missing_files()
        indexed_message_count = state_store.count_messages()

        plans = plan_mailboxes_concurrently(
            credentials,
            mailboxes,
            state_store,
            force_full_scan,
            indexed_message_count,
            stats,
            status,
            progress,
        )
        reset_eta_sample(stats)

        progress.update(force=True)

        processed_mailboxes: list[str] = []
        for plan in plans:
            try:
                sync_mailbox(
                    connection,
                    credentials,
                    plan,
                    email_root,
                    existing_files,
                    state_store,
                    stats,
                    status,
                    progress,
                )
                processed_mailboxes.append(plan.mailbox.display_name)
            except Exception as exc:
                status.line()
                stats["errors"] += 1
                print(f"Mailbox error: {exc}")

        status.done()
        state_store.commit()

        summary = {
            **stats,
            "removed_part_files": removed_part_files,
            "mailboxes_processed": processed_mailboxes,
            "total_messages_indexed": state_store.count_messages(),
        }
        summary.pop("metadata_batch_successes", None)
        summary.pop("active_downloaded", None)
        summary.pop("active_downloaded_bytes", None)
        summary.pop("active_archived", None)
        summary.pop("eta_started_at", None)
        summary.pop("eta_baseline_done", None)
        summary.pop("eta_smoothed_remaining", None)
        summary.pop("transfer_rate_last_at", None)
        summary.pop("transfer_rate_last_bytes", None)
        summary.pop("transfer_rate_last_progress_at", None)
        summary.pop("transfer_rate_bytes_per_second", None)
        return summary
    finally:
        progress.stop()
        status.done()
        if state_store is not None:
            state_store.close()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = CompactHelpParser(
        description="Download raw Gmail messages to emails/raw as idempotent .eml files.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="path to credential JSON",
    )
    parser.add_argument(
        "--emails-dir",
        "--output-dir",
        dest="emails_dir",
        type=Path,
        default=DEFAULT_EMAIL_ROOT,
        help="archive output directory",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    config_path = args.config.expanduser().resolve()
    email_root = args.emails_dir.expanduser().resolve()
    started_timestamp = datetime.now().timestamp()
    started_at = iso_now()
    connection: imaplib.IMAP4_SSL | None = None

    try:
        credentials, connection = get_authenticated_connection(config_path)
        summary = download_archive(credentials, connection, email_root)
        completed_at = iso_now()
        elapsed_seconds = elapsed_since(started_timestamp)
        sync_state = {
            "last_started_at": started_at,
            "last_completed_at": completed_at,
            "last_status": "success" if summary["errors"] == 0 else "completed_with_errors",
            "elapsed_seconds": round(elapsed_seconds, 3),
            **summary,
        }
        write_sync_state(email_root, sync_state)

        print_final_summary(summary, email_root, elapsed_seconds)
        return 0 if summary["errors"] == 0 else 1

    except KeyboardInterrupt:
        removed_part_files = cleanup_part_files(email_root)
        elapsed_seconds = elapsed_since(started_timestamp)
        if removed_part_files:
            print(
                f"\nInterrupted after {format_duration(elapsed_seconds)}. "
                f"Removed {removed_part_files} partial download files."
            )
        else:
            print(f"\nInterrupted after {format_duration(elapsed_seconds)}.")
        write_sync_state(
            email_root,
            {
                "last_started_at": started_at,
                "last_completed_at": iso_now(),
                "last_status": "interrupted",
                "elapsed_seconds": round(elapsed_seconds, 3),
                "removed_part_files": removed_part_files,
            },
        )
        return 130
    except NetworkValidationError as exc:
        print(f"Network error: {exc}")
        print("Credentials were not saved or changed because they could not be tested.")
        return 2
    except RuntimeError as exc:
        elapsed_seconds = elapsed_since(started_timestamp)
        print(f"Error: {exc}")
        write_sync_state(
            email_root,
            {
                "last_started_at": started_at,
                "last_completed_at": iso_now(),
                "last_status": "failed",
                "elapsed_seconds": round(elapsed_seconds, 3),
                "error": str(exc),
            },
        )
        return 1
    finally:
        close_connection(connection)
        cleanup_part_files(email_root)


if __name__ == "__main__":
    raise SystemExit(main())
