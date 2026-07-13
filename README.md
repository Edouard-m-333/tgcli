# tgcli

Telegram CLI tool in **pure Rust** using [grammers](https://github.com/Lonami/grammers) (MTProto). No TDLib, no C/C++ dependencies. `cargo build` and done.

> **Fork notice** — this is a fork of [dgrr/tgcli](https://github.com/dgrr/tgcli) with extra features on top of upstream. See [What's in this fork](#whats-in-this-fork).

## What's in this fork

Features added on top of upstream `dgrr/tgcli`:

- **Full-history backfill** — `messages fetch --all` walks a chat back to its very first message (Ctrl-C safe, resumes where it left off). Supports `--download-media` and repeatable `--chat` to backfill several chats in one run. See [Backfill](#backfill-full-history).
- **Contacts auto-populated from message senders** — every message processed by `sync`, the daemon, or `chats members` upserts its sender into the local contacts table. Group members outside your address book get real names instead of `user:<id>`, at no extra API cost.
- **Daemon startup catch-up** — on (re)start the daemon runs one incremental sync of chats active in the last 30 days before entering the live loop, recovering messages missed while it was down. Opt out with `--no-startup-catchup`.

## Quick Install

Requires [Rust](https://rustup.rs/) (`cargo`). Prebuilt binaries (Homebrew, install script) are only published by upstream and **do not include the fork features** — build this fork from source.

### Install directly with cargo (recommended for a new device)

```bash
cargo install --git https://github.com/Edouard-m-333/tgcli --locked
```

This builds and places `tgcli` in `~/.cargo/bin/` (make sure it is on your `PATH`).

### Clone and build

```bash
git clone https://github.com/Edouard-m-333/tgcli.git
cd tgcli
cargo build --release
cp target/release/tgcli /usr/local/bin/
```

### Upstream version (without fork features)

```bash
brew install dgrr/tgcli/tgcli
# or
curl -fsSL https://raw.githubusercontent.com/dgrr/tgcli/main/install.sh | bash
```

## Features

- **Auth**: Phone → code → 2FA authentication
- **Sync**: Incremental sync with checkpoints, stored in libSQL (turso) with FTS5
- **Chats**: List, search, create, join/leave, archive, pin, mute
- **Messages**: List, search (FTS5 + global API), send, edit, delete, forward, download
- **Backfill**: Fetch older history per chat — up to the full history with `--all`, media included with `--download-media` *(fork)*
- **Contacts**: List and search from local DB — auto-populated from message senders *(fork)*
- **Admin**: Ban, kick, promote, demote group members
- **Read**: Mark messages as read
- **Stickers**: List, search, send stickers
- **Polls**: Create polls
- **Profile**: Show and update your profile
- **Folders**: Create and manage chat folders
- **Output**: Human-readable tables or `--json`

## Quick Start

```bash
# Authenticate
tgcli auth

# Sync messages (incremental by default)
tgcli sync

# Full sync (first time or refresh)
tgcli sync --full

# List chats
tgcli chats list

# Search messages locally (FTS5)
tgcli messages search "hello"

# Search messages globally (Telegram API)
tgcli messages search --global "hello"

# Send a message
tgcli send --to <chat_id> --message "Hello!"

# Download media from a message
tgcli messages download --chat <chat_id> --message <msg_id>
```

## Sync Behavior

- **First run**: Fetches all chats + last 50 messages per chat (configurable with `--messages-per-chat`)
- **Subsequent runs**: Pure incremental sync — only fetches new messages since last checkpoint
- **`--full`**: Forces a full sync, ignoring checkpoints

```bash
# Default incremental sync
tgcli sync

# Full sync with 100 messages per chat
tgcli sync --full --messages-per-chat 100

# Sync with progress suppressed
tgcli sync --no-progress

# Output as JSONL stream
tgcli sync --stream
```

## Backfill (Full History)

`sync` only moves forward from its checkpoint. To fetch **older** messages, use `messages fetch` — it walks backward from the oldest message already stored, so re-running always resumes where the last run stopped.

```bash
# Fetch the next 100 older messages of a chat (default)
tgcli messages fetch --chat 987654321

# Fetch a specific amount
tgcli messages fetch --chat 987654321 --limit 1000

# Fetch the ENTIRE history of a chat (walks back to the first message)
tgcli messages fetch --chat 987654321 --all

# Download media while backfilling (same as sync --download-media)
tgcli messages fetch --chat 987654321 --all --download-media

# Backfill several chats in one run (--chat is repeatable)
tgcli messages fetch --chat 111 --chat 222 --chat 333 --all

# Forum groups: restrict to a topic
tgcli messages fetch --chat 987654321 --topic 42
```

Notes:

- `--all` ignores `--limit` and keeps going until the start of the chat.
- Every message is stored immediately, so **Ctrl-C is safe** — a long walk can be interrupted and resumed later with the same command.
- Very large chats can hit Telegram rate limits (FLOOD_WAIT); since progress is saved continuously, you can interrupt and resume at any time.

## Daemon (Optional)

The `daemon` command is **optional** and only needed for real-time message capture.

**When to use `sync` (most use cases):**
- Periodic message fetching (cron, on-demand)
- Catching up on missed messages
- One-time data export
- CLI workflows and scripts

**When to use `daemon`:**
- Instant notifications as messages arrive
- Real-time message processing pipelines
- Live message streaming to external systems
- Continuous monitoring of specific chats

```bash
# Start daemon (listens for real-time updates)
tgcli daemon

# Daemon with JSONL output (for pipelines)
tgcli daemon --stream

# Skip background sync (pure real-time only)
tgcli daemon --no-backfill

# Ignore specific chats or all channels
tgcli daemon --ignore 123456789 --ignore-channels

# Skip the startup catch-up sync (fork feature, see below)
tgcli daemon --no-startup-catchup
```

The daemon maintains a persistent connection to Telegram and stores messages instantly as they arrive.

**Startup catch-up** *(fork)*: on every (re)start, the daemon first runs a one-shot incremental sync of chats active within the last 30 days, recovering messages missed while it was down (reboot, crash, reconnect). It runs sequentially before the live stream starts, so it never contends with the live writer for the DB lock. Disable with `--no-startup-catchup`.

## Architecture

```
src/
  main.rs          CLI entry point (clap)
  cmd/             Command handlers
    auth.rs        Phone → code → 2FA
    sync.rs        Incremental/full sync
    chats.rs       List/search/create/join/leave/archive/pin/mute
    messages.rs    List/search/send/edit/delete/forward/download
    send.rs        Send text/files/voice/video
    contacts.rs    List/search contacts
    read.rs        Mark as read
    stickers.rs    List/search/send stickers
    polls.rs       Create polls
    profile.rs     Show/update profile
    folders.rs     Create/delete folders
    users.rs       Show/block/unblock users
    typing.rs      Send typing indicator
    completions.rs Shell completions
  store/           turso (libSQL) + FTS5 storage
  tg/              grammers client wrapper
  app/             App struct + business logic
  out/             Output formatting
```

## Storage

- Session: `~/.tgcli/session.db` (grammers SqliteSession)
- Data: `~/.tgcli/tgcli.db` (chats, contacts, messages + FTS5)

Multi-account support via `--store`:
```bash
tgcli --store ~/.tgcli-work sync
tgcli --store ~/.tgcli-personal sync
```

Reset local database (keeps session):
```bash
tgcli wipe        # Asks for confirmation
tgcli wipe --yes  # Skip confirmation
```

## Shell Completions

```bash
# Bash
tgcli completions bash > /etc/bash_completion.d/tgcli

# Zsh
tgcli completions zsh > ~/.zfunc/_tgcli

# Fish
tgcli completions fish > ~/.config/fish/completions/tgcli.fish
```

## Why Rust?

The Go version (`tgcli-go`) uses TDLib (C++), requiring complex cross-compilation and system dependencies. `tgcli` is pure Rust — zero C/C++ deps, single `cargo build`, tiny binary.

Uses [turso](https://github.com/tursodatabase/libsql) for database storage — a pure Rust libSQL implementation with no native compilation required.

## License

MIT
