# Gmail Downloader

Download a Gmail mailbox to local raw `.eml` files with idempotency. The script uses Gmail IMAP and an app password, stores each message once, and can be rerun to download only missing messages.

Each `.eml` file is the original raw email message. Attachments and inline images stay inside the `.eml` file and can be extracted later with an email client or a MIME parser.

## Requirements

- Python 3.10 or newer
- Gmail account with IMAP enabled
- Gmail app password

No Python packages need to be installed. The script uses only the Python standard library.

## Performance

Downloads use parallel IMAP workers by default. The worker count is chosen from the machine CPU count and capped at 12 worker connections, leaving room for the main metadata connection and other mail clients.

The script also:

- Fetches message metadata in batches.
- Downloads missing raw emails in parallel.
- Processes Gmail UIDs from highest to lowest so newer messages are queued first.
- Computes SHA-256 while writing each `.eml` file.
- Stores per-mailbox resume markers so later runs skip already-scanned UID ranges.
- Shows live progress while scanning and downloading.
- Removes stale `.part` files at startup and on exit.

Google documents a Gmail IMAP download bandwidth limit of 2500 MB per day for Workspace accounts: <https://support.google.com/a/answer/1071518>.

## Quick Start

Run from a cloned repo:

```bash
python3 scripts/email/downloader.py
```

On first run, the script prompts for:

- Gmail address
- Gmail app password

The app password may be pasted with spaces, like Google displays it. The script removes the spaces before validation and saving.

After a successful Gmail IMAP login test, the script asks whether to save credentials for future runs. If you answer no, nothing is saved and you will be prompted again next time.

## Output

Default output after running from any folder:

```text
emails/
  raw/
    2026-04-30__187fabc123456789.eml
  _state/
    message_index.json
    sync_state.json
```

By default, downloaded emails are written to `./emails` relative to the directory where you run the command.

If you choose to save credentials, the default config path is:

```text
~/.config/gmail-downloader/email.json
```

The credential file contains only:

```json
{
  "email": "business@example.com",
  "app_password": "xxxxxxxxxxxxxxxx"
}
```

Do not commit credential files or downloaded emails.

## Idempotency

The script uses Gmail's stable `X-GM-MSGID` value to avoid duplicate downloads.

On rerun:

- Existing indexed `.eml` files are skipped.
- Missing `.eml` files are downloaded again.
- Messages that appear in multiple Gmail labels are stored once.
- Label/mailbox sightings are merged into `emails/_state/message_index.json`.

## Standalone Script Use

You can copy only the script to another folder and run it there:

```bash
cp scripts/email/downloader.py ~/gmail-downloader.py
python3 ~/gmail-downloader.py
```

When copied outside the repo layout, the script still behaves like a normal CLI:

```text
~/.config/gmail-downloader/email.json
./emails/raw/*.eml
./emails/_state/*.json
```

You can override paths:

```bash
python3 scripts/email/downloader.py \
  --config "$HOME/.config/gmail-downloader/email.json" \
  --emails-dir "$HOME/gmail-archive"
```

`--output-dir` is also accepted as an alias for `--emails-dir`.

You can also use environment variables:

```bash
GMAIL_DOWNLOADER_CONFIG="$HOME/private/gmail.json" \
GMAIL_DOWNLOADER_EMAILS_DIR="$HOME/gmail-archive" \
python3 scripts/email/downloader.py
```

To override the automatic worker count without adding a command-line option:

```bash
GMAIL_DOWNLOADER_WORKERS=8 python3 scripts/email/downloader.py
```

## Gmail Setup Notes

For Gmail app passwords:

1. Enable 2-Step Verification on the Google account.
2. Create an app password in the Google account security settings.
3. Enable IMAP in Gmail settings if it is disabled.

If login fails, likely causes are:

- Wrong app password
- 2-Step Verification is not enabled
- IMAP is disabled
- Google Workspace policy blocks app passwords or IMAP

## What This Does Not Do

This version intentionally stores raw `.eml` files only. It does not extract attachments, parse bodies, or create readable HTML/text exports.

## Security

Credential saving is opt-in. If you choose to save credentials, the script writes the JSON file with user-only file permissions where supported. Keep the config file private because an app password can access the mailbox.
