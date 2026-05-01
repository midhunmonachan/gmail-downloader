#!/usr/bin/env python3
"""Download raw Gmail messages as idempotent .eml files."""

from __future__ import annotations

import argparse
import getpass
import hashlib
import imaplib
import json
import os
import re
import socket
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Any


IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993
IMAP_TIMEOUT_SECONDS = 30
APP_NAME = "gmail-downloader"
METADATA_BATCH_SIZE = 100
GMAIL_IMAP_CONNECTION_LIMIT = 15
RESERVED_IMAP_CONNECTIONS = 2
MAIN_IMAP_CONNECTIONS = 1
MAX_DOWNLOAD_WORKERS = max(1, GMAIL_IMAP_CONNECTION_LIMIT - RESERVED_IMAP_CONNECTIONS - MAIN_IMAP_CONNECTIONS)
STATUS_REFRESH_SECONDS = 0.2


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


@dataclass(frozen=True)
class DownloadOutcome:
    pending: PendingDownload
    byte_size: int | None
    sha256: str | None
    downloaded_at: str | None
    error: str | None


class CredentialError(RuntimeError):
    """Raised when credential validation should be retried."""


class NetworkValidationError(RuntimeError):
    """Raised when credentials cannot be tested because Gmail is unreachable."""


class LiveStatus:
    def __init__(self) -> None:
        self.enabled = sys.stdout.isatty()
        self.last_message = ""
        self.last_update = 0.0

    def update(self, message: str, *, force: bool = False) -> None:
        now = datetime.now().timestamp()
        if not force and now - self.last_update < STATUS_REFRESH_SECONDS:
            return
        previous = self.last_message
        self.last_message = message
        self.last_update = now
        if self.enabled:
            width = max(len(message), len(previous))
            print("\r" + message.ljust(width), end="", flush=True)
        else:
            print(message, flush=True)

    def line(self, message: str = "") -> None:
        if self.enabled and self.last_message:
            print("\r" + " " * len(self.last_message) + "\r", end="", flush=True)
            self.last_message = ""
        if message:
            print(message, flush=True)

    def done(self) -> None:
        if self.enabled and self.last_message:
            print()
            self.last_message = ""


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


def search_uids(connection: imaplib.IMAP4_SSL, *, start_uid: int | None = None) -> list[bytes]:
    if start_uid is None:
        status, data = connection.uid("SEARCH", None, "ALL")
    else:
        status, data = connection.uid("SEARCH", None, "UID", f"{start_uid}:*")

    if status != "OK":
        raise RuntimeError(f"Could not search selected mailbox: {data!r}")
    if not data or not data[0]:
        return []
    return data[0].split()


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


def batched(items: list[bytes], size: int) -> list[list[bytes]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def filename_for_message(metadata: MessageMetadata) -> str:
    date_part = metadata.internal_date.date().isoformat()
    return f"{date_part}__{metadata.gmail_msg_id}.eml"


def find_existing_message_file(raw_dir: Path, gmail_msg_id: str) -> Path | None:
    matches = sorted(raw_dir.glob(f"*__{gmail_msg_id}.eml"))
    if matches:
        return matches[0]
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
        "sha256": sha256_file(path),
        "last_seen_at": iso_now(),
    }

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


def empty_message_index() -> dict[str, Any]:
    return {
        "version": 1,
        "updated_at": iso_now(),
        "messages": {},
        "mailboxes": {},
    }


def load_message_index(path: Path) -> dict[str, Any]:
    index = load_json(path, empty_message_index(), strict=True)
    if not isinstance(index, dict):
        raise RuntimeError(f"{path} must contain a JSON object.")
    if not isinstance(index.get("messages"), dict):
        index["messages"] = {}
    if "version" not in index:
        index["version"] = 1
    return index


def save_message_index(path: Path, index: dict[str, Any]) -> None:
    index["updated_at"] = iso_now()
    write_json_atomic(path, index)


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


def indexed_file_missing(email_root: Path, index: dict[str, Any]) -> bool:
    messages = index.get("messages")
    if not isinstance(messages, dict):
        return False

    for entry in messages.values():
        if isinstance(entry, dict) and message_file_exists(email_root, entry) is None:
            return True
    return False


def mailbox_states(index: dict[str, Any]) -> dict[str, dict[str, Any]]:
    states = index.setdefault("mailboxes", {})
    if not isinstance(states, dict):
        states = {}
        index["mailboxes"] = states
    return states


def mailbox_state(index: dict[str, Any], mailbox_name: str) -> dict[str, Any]:
    states = mailbox_states(index)
    state = states.setdefault(mailbox_name, {})
    if not isinstance(state, dict):
        state = {}
        states[mailbox_name] = state
    return state


def int_from_state(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def max_uid_value(uids: list[bytes]) -> int:
    return max(int(uid) for uid in uids)


def restore_existing_labels(replacement: dict[str, Any], previous_entry: dict[str, Any] | None) -> None:
    if previous_entry is None:
        return

    labels = previous_entry.get("labels", [])
    if not isinstance(labels, list):
        return

    for label in labels:
        if isinstance(label, str):
            merge_label(replacement, label)


def download_worker(
    credentials: Credentials,
    mailbox: Mailbox,
    task_queue: Queue[PendingDownload | None],
    result_queue: Queue[DownloadOutcome],
) -> None:
    connection: imaplib.IMAP4_SSL | None = None
    connection_error: str | None = None

    try:
        connection = connect_and_login(credentials)
        select_mailbox(connection, mailbox)
    except Exception as exc:
        connection_error = str(exc)

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

                if connection is None:
                    result_queue.put(
                        DownloadOutcome(
                            pending=pending,
                            byte_size=None,
                            sha256=None,
                            downloaded_at=None,
                            error="download worker did not open an IMAP connection",
                        )
                    )
                    continue

                raw_message = fetch_raw_message(connection, pending.uid)
                byte_size, digest = write_raw_message(pending.final_path, raw_message)
                result_queue.put(
                    DownloadOutcome(
                        pending=pending,
                        byte_size=byte_size,
                        sha256=digest,
                        downloaded_at=iso_now(),
                        error=None,
                    )
                )
            except Exception as exc:
                if pending is not None:
                    result_queue.put(
                        DownloadOutcome(
                            pending=pending,
                            byte_size=None,
                            sha256=None,
                            downloaded_at=None,
                            error=str(exc),
                        )
                    )
            finally:
                task_queue.task_done()
    finally:
        close_connection(connection)


def download_pending_messages(
    credentials: Credentials,
    mailbox: Mailbox,
    pending_items: list[PendingDownload],
    status: LiveStatus,
) -> list[DownloadOutcome]:
    if not pending_items:
        return []

    worker_count = min(DEFAULT_DOWNLOAD_WORKERS, len(pending_items))
    task_queue: Queue[PendingDownload | None] = Queue()
    result_queue: Queue[DownloadOutcome] = Queue()
    outcomes: list[DownloadOutcome] = []
    downloaded_bytes = 0

    threads = [
        Thread(
            target=download_worker,
            args=(credentials, mailbox, task_queue, result_queue),
            daemon=True,
        )
        for _ in range(worker_count)
    ]
    for thread in threads:
        thread.start()

    for pending in pending_items:
        task_queue.put(pending)
    for _ in threads:
        task_queue.put(None)

    status.update(
        f"{mailbox.display_name}: downloading 0/{len(pending_items)} messages with {worker_count} workers",
        force=True,
    )

    while len(outcomes) < len(pending_items):
        outcome = result_queue.get()
        outcomes.append(outcome)
        if outcome.byte_size is not None:
            downloaded_bytes += outcome.byte_size
        status.update(
            f"{mailbox.display_name}: downloaded {len(outcomes)}/{len(pending_items)} pending messages "
            f"({format_bytes(downloaded_bytes)})",
            force=True,
        )

    task_queue.join()
    for thread in threads:
        thread.join()

    return outcomes


def sync_mailbox(
    connection: imaplib.IMAP4_SSL,
    credentials: Credentials,
    mailbox: Mailbox,
    email_root: Path,
    index: dict[str, Any],
    stats: dict[str, int],
    status: LiveStatus,
    *,
    force_full_scan: bool,
) -> None:
    raw_dir = email_root / "raw"
    message_count, uidvalidity = select_mailbox(connection, mailbox)
    state = mailbox_state(index, mailbox.display_name)
    last_seen_uid = int_from_state(state.get("last_seen_uid"))
    can_resume = (
        not force_full_scan
        and uidvalidity is not None
        and state.get("uidvalidity") == uidvalidity
        and last_seen_uid is not None
    )
    start_uid = last_seen_uid + 1 if can_resume else None

    if can_resume:
        status.line(f"Mailbox: {mailbox.display_name} ({message_count} messages, resuming after UID {last_seen_uid})")
    else:
        status.line(f"Mailbox: {mailbox.display_name} ({message_count} messages, full scan)")

    uids = search_uids(connection, start_uid=start_uid)
    messages: dict[str, dict[str, Any]] = index["messages"]
    dirty_count = 0

    if not uids:
        if uidvalidity is not None:
            state["uidvalidity"] = uidvalidity
        state["last_completed_at"] = iso_now()
        save_message_index(email_root / "_state" / "message_index.json", index)
        status.line(f"  No new messages in {mailbox.display_name}")
        return

    for batch_number, uid_batch in enumerate(batched(uids, METADATA_BATCH_SIZE), start=1):
        batch_failed = False
        pending_downloads: list[PendingDownload] = []

        status.update(
            f"{mailbox.display_name}: reading metadata batch {batch_number} "
            f"({min(batch_number * METADATA_BATCH_SIZE, len(uids))}/{len(uids)} UIDs)",
            force=True,
        )

        try:
            metadata_by_uid = fetch_metadata_batch(connection, uid_batch)
        except Exception as exc:
            status.line(f"  Metadata batch failed in {mailbox.display_name}: {exc}")
            metadata_by_uid = {}
            for uid in uid_batch:
                try:
                    metadata_by_uid[uid] = fetch_message_metadata(connection, uid)
                except Exception as item_exc:
                    stats["errors"] += 1
                    batch_failed = True
                    uid_text = uid.decode("ascii", "replace")
                    status.line(f"  Error on {mailbox.display_name} UID {uid_text}: {item_exc}")

        for uid in uid_batch:
            metadata = metadata_by_uid.get(uid)
            if metadata is None:
                continue

            entry = messages.get(metadata.gmail_msg_id)

            if isinstance(entry, dict):
                existing_path = message_file_exists(email_root, entry)
                if existing_path is not None:
                    if merge_label(entry, mailbox.display_name):
                        stats["labels_merged"] += 1
                        dirty_count += 1
                    entry["last_seen_at"] = iso_now()
                    stats["skipped"] += 1
                    continue

                existing_by_id = find_existing_message_file(raw_dir, metadata.gmail_msg_id)
                if existing_by_id is not None:
                    replacement = index_entry_for_file(
                        metadata,
                        existing_by_id,
                        email_root,
                        mailbox.display_name,
                        downloaded_at=entry.get("downloaded_at"),
                    )
                    labels = entry.get("labels", [])
                    if isinstance(labels, list):
                        for label in labels:
                            if isinstance(label, str):
                                merge_label(replacement, label)
                    messages[metadata.gmail_msg_id] = replacement
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
                existing_by_id = find_existing_message_file(raw_dir, metadata.gmail_msg_id)
                if existing_by_id is not None:
                    messages[metadata.gmail_msg_id] = index_entry_for_file(
                        metadata,
                        existing_by_id,
                        email_root,
                        mailbox.display_name,
                        downloaded_at=None,
                    )
                    stats["indexed_existing"] += 1
                    dirty_count += 1
                    continue

            filename = filename_for_message(metadata)
            final_path = raw_dir / filename

            if final_path.exists():
                messages[metadata.gmail_msg_id] = index_entry_for_file(
                    metadata,
                    final_path,
                    email_root,
                    mailbox.display_name,
                    downloaded_at=None,
                )
                stats["indexed_existing"] += 1
                dirty_count += 1
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

        for outcome in download_pending_messages(credentials, mailbox, pending_downloads, status):
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
            messages[pending.metadata.gmail_msg_id] = entry
            stats["downloaded_bytes"] += outcome.byte_size
            if pending.restoring:
                stats["restored_missing"] += 1
            else:
                stats["downloaded"] += 1
            dirty_count += 1

        if batch_failed:
            save_message_index(email_root / "_state" / "message_index.json", index)
            status.line(
                f"  Stopped {mailbox.display_name} after a failed batch. "
                "Next run will resume from the last completed UID."
            )
            return

        if uidvalidity is not None:
            state["uidvalidity"] = uidvalidity
        state["last_seen_uid"] = max_uid_value(uid_batch)
        state["last_completed_at"] = iso_now()
        dirty_count += 1

        status.update(
            f"{mailbox.display_name}: done {min(batch_number * METADATA_BATCH_SIZE, len(uids))}/{len(uids)} UIDs "
            f"downloaded={stats['downloaded']} restored={stats['restored_missing']} "
            f"bytes={format_bytes(stats['downloaded_bytes'])} skipped={stats['skipped']} errors={stats['errors']}",
            force=True,
        )

        if dirty_count >= 50:
            save_message_index(email_root / "_state" / "message_index.json", index)
            dirty_count = 0

    if dirty_count:
        save_message_index(email_root / "_state" / "message_index.json", index)
    status.line(f"  Completed {mailbox.display_name}: {len(uids)} UIDs checked")


def download_archive(credentials: Credentials, connection: imaplib.IMAP4_SSL, email_root: Path) -> dict[str, Any]:
    email_root.mkdir(parents=True, exist_ok=True)
    (email_root / "_state").mkdir(parents=True, exist_ok=True)
    (email_root / "raw").mkdir(parents=True, exist_ok=True)
    status = LiveStatus()

    removed_part_files = cleanup_part_files(email_root)
    if removed_part_files:
        status.line(f"Removed {removed_part_files} stale partial download files.")

    index_path = email_root / "_state" / "message_index.json"
    index = load_message_index(index_path)
    mailboxes = list_selectable_mailboxes(connection)
    if not mailboxes:
        raise RuntimeError("No selectable Gmail mailboxes were found.")
    force_full_scan = indexed_file_missing(email_root, index)
    if force_full_scan:
        status.line("Missing indexed .eml files found; using full scans so missing messages can be restored.")

    stats = {
        "downloaded": 0,
        "downloaded_bytes": 0,
        "skipped": 0,
        "restored_missing": 0,
        "indexed_existing": 0,
        "labels_merged": 0,
        "errors": 0,
    }

    processed_mailboxes: list[str] = []
    for mailbox in mailboxes:
        try:
            sync_mailbox(
                connection,
                credentials,
                mailbox,
                email_root,
                index,
                stats,
                status,
                force_full_scan=force_full_scan,
            )
            processed_mailboxes.append(mailbox.display_name)
        except Exception as exc:
            status.line()
            stats["errors"] += 1
            print(f"Mailbox error for {mailbox.display_name}: {exc}")

    status.done()
    save_message_index(index_path, index)

    return {
        **stats,
        "removed_part_files": removed_part_files,
        "mailboxes_processed": processed_mailboxes,
        "total_messages_indexed": len(index["messages"]),
    }


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
    started_at = iso_now()
    connection: imaplib.IMAP4_SSL | None = None

    try:
        credentials, connection = get_authenticated_connection(config_path)
        summary = download_archive(credentials, connection, email_root)
        completed_at = iso_now()
        sync_state = {
            "last_started_at": started_at,
            "last_completed_at": completed_at,
            "last_status": "success" if summary["errors"] == 0 else "completed_with_errors",
            **summary,
        }
        write_sync_state(email_root, sync_state)

        print("\nDone.")
        print(f"  Downloaded: {summary['downloaded']}")
        print(f"  Downloaded bytes: {format_bytes(summary['downloaded_bytes'])}")
        print(f"  Skipped: {summary['skipped']}")
        print(f"  Restored missing: {summary['restored_missing']}")
        print(f"  Indexed existing: {summary['indexed_existing']}")
        print(f"  Errors: {summary['errors']}")
        print(f"  Partial files cleaned: {summary['removed_part_files']}")
        print(f"  Raw emails: {email_root / 'raw'}")
        return 0 if summary["errors"] == 0 else 1

    except KeyboardInterrupt:
        removed_part_files = cleanup_part_files(email_root)
        print(f"\nInterrupted. Removed {removed_part_files} partial download files.")
        write_sync_state(
            email_root,
            {
                "last_started_at": started_at,
                "last_completed_at": iso_now(),
                "last_status": "interrupted",
                "removed_part_files": removed_part_files,
            },
        )
        return 130
    except NetworkValidationError as exc:
        print(f"Network error: {exc}")
        print("Credentials were not saved or changed because they could not be tested.")
        return 2
    except RuntimeError as exc:
        print(f"Error: {exc}")
        write_sync_state(
            email_root,
            {
                "last_started_at": started_at,
                "last_completed_at": iso_now(),
                "last_status": "failed",
                "error": str(exc),
            },
        )
        return 1
    finally:
        close_connection(connection)
        cleanup_part_files(email_root)


if __name__ == "__main__":
    raise SystemExit(main())
