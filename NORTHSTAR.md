# Northstar macOS contract

This fork's `v0.3.8-northstar.1` release provides the `northstar-macos-v1`
contract for Northstar's packaged Mac installer. It includes the previously
published durable live capture and three-hour reconciliation fixes.

`tgcli --output json version` identifies the contract. While `daemon` is
running, `northstar-health.json` in the configured store reports a direct
Telegram identity/connectivity probe and local archive health every 30 seconds.
It contains no credentials or message content. A missing or stale report is
not evidence of a working connection.

Northstar writes selected numeric chat IDs to `northstar-history-request.json`.
The daemon imports one bounded older-history page at a time on its existing
writer and persists progress in `northstar-history.json`. This is resumable;
unselected message content remains local. Northstar independently verifies
that each completed history import has also uploaded successfully.

Build the Mac artifacts with a Rust toolchain and the Apple command-line tools:

```sh
MACOSX_DEPLOYMENT_TARGET=14.0 cargo build --locked --release --target aarch64-apple-darwin
MACOSX_DEPLOYMENT_TARGET=14.0 cargo build --locked --release --target x86_64-apple-darwin
```

Publish `tgcli-darwin-arm64` and `tgcli-darwin-x64` with SHA-256 checksums from
this exact commit. Northstar pins both hashes. Fresh-Mac Telegram authentication,
restart, offline recovery and large-history testing remain separate release
acceptance checks; cross-compilation does not replace an Intel-Mac pilot.
