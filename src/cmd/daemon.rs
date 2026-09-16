//! Daemon mode for persistent real-time message sync.
//!
//! This command starts a persistent Telegram client that:
//! 1. Immediately subscribes to real-time updates
//! 2. Saves incoming messages to the local database as they arrive
//! 3. Optionally runs background incremental sync to catch up on missed messages

use crate::app::App;
use crate::shutdown;
use crate::store::{Store, UpsertMessageParams};
use crate::Cli;
use anyhow::{Context, Result};
use chrono::Utc;
use clap::Args;
use grammers_client::types::Peer;
use grammers_client::{Update, UpdatesConfiguration};
use grammers_session::defs::{PeerId, PeerKind};
use grammers_tl_types as tl;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::MissedTickBehavior;

/// The startup catch-up only re-syncs chats active within this many days, so it
/// polls a few hundred chats at most instead of every chat (avoids FLOOD_WAIT).
const STARTUP_CATCHUP_DAYS: i64 = 30;

/// Reconcile dialog metadata and recent history every three hours. Live
/// updates remain the fast path; this overlap is the durable repair path.
const DEFAULT_RECONCILE_INTERVAL_SECONDS: u64 = 3 * 60 * 60;

#[derive(Args, Debug, Clone)]
pub struct DaemonArgs {
    /// Don't run background sync (only listen for new updates)
    #[arg(long, default_value_t = true)] // Default to true to avoid lock conflicts on startup
    pub no_backfill: bool,

    /// Download media files for incoming messages
    #[arg(long, default_value_t = false)]
    pub download_media: bool,

    /// Chat IDs to ignore (skip during sync and updates)
    #[arg(long = "ignore", value_name = "CHAT_ID")]
    pub ignore_chat_ids: Vec<i64>,

    /// Skip all channel updates
    #[arg(long, default_value_t = false)]
    pub ignore_channels: bool,

    /// Suppress progress output
    #[arg(long, default_value_t = false)]
    pub quiet: bool,

    /// Output updates as JSONL stream to stdout
    #[arg(long, default_value_t = false)]
    pub stream: bool,

    /// Skip the one-shot incremental catch-up sync that runs at startup
    /// (which recovers messages missed while the daemon was down).
    #[arg(long, default_value_t = false)]
    pub no_startup_catchup: bool,

    /// Periodically refresh dialogs and reconcile recent history. Set to 0 to
    /// disable. This remains active when --no-backfill disables the older
    /// concurrent background sync.
    #[arg(long, default_value_t = DEFAULT_RECONCILE_INTERVAL_SECONDS)]
    pub reconcile_interval_seconds: u64,
}

/// Extract sender_id from a Message update
fn extract_sender_id(msg: &grammers_client::types::update::Message) -> i64 {
    if let Some(sender) = msg.sender() {
        return sender.id().bare_id();
    }

    let raw_sender = match message_from_raw_update(&msg.raw) {
        Some(tl::enums::Message::Message(message)) => message.from_id.as_ref(),
        Some(tl::enums::Message::Service(message)) => message.from_id.as_ref(),
        Some(tl::enums::Message::Empty(_)) | None => None,
    };
    if let Some(sender) = raw_sender {
        return bare_id_from_raw_peer(sender);
    }

    let peer_id = msg.peer_id();
    if !msg.outgoing() && matches!(peer_id.kind(), PeerKind::User | PeerKind::UserSelf) {
        return peer_id.bare_id();
    }

    0
}

fn message_from_raw_update(raw: &tl::enums::Update) -> Option<&tl::enums::Message> {
    match raw {
        tl::enums::Update::NewMessage(update) => Some(&update.message),
        tl::enums::Update::NewChannelMessage(update) => Some(&update.message),
        tl::enums::Update::EditMessage(update) => Some(&update.message),
        tl::enums::Update::EditChannelMessage(update) => Some(&update.message),
        _ => None,
    }
}

fn bare_id_from_raw_peer(peer: &tl::enums::Peer) -> i64 {
    match peer {
        tl::enums::Peer::User(user) => user.user_id,
        tl::enums::Peer::Chat(chat) => chat.chat_id,
        tl::enums::Peer::Channel(channel) => channel.channel_id,
    }
}

fn chat_kind_from_peer_id(peer_id: PeerId) -> &'static str {
    match peer_id.kind() {
        PeerKind::User | PeerKind::UserSelf => "user",
        PeerKind::Chat => "group",
        PeerKind::Channel => "channel",
    }
}

/// Extract topic_id from a raw update if present
fn extract_topic_id_from_raw(raw: &tl::enums::Update) -> Option<i32> {
    match raw {
        tl::enums::Update::NewChannelMessage(m) => extract_topic_from_message(&m.message),
        tl::enums::Update::EditChannelMessage(m) => extract_topic_from_message(&m.message),
        _ => None,
    }
}

fn extract_topic_from_message(msg: &tl::enums::Message) -> Option<i32> {
    if let tl::enums::Message::Message(m) = msg {
        if let Some(tl::enums::MessageReplyHeader::Header(header)) = &m.reply_to {
            if header.forum_topic {
                return header.reply_to_top_id.or(header.reply_to_msg_id);
            }
        }
    }
    None
}

/// Determine chat kind from peer
fn chat_kind_from_peer(peer: &Peer) -> &'static str {
    match peer {
        Peer::User(_) => "user",
        Peer::Group(_) => "group",
        Peer::Channel(c) => {
            // Channel.raw.megagroup indicates if this is actually a megagroup (supergroup)
            if c.raw.megagroup {
                "group"
            } else {
                "channel"
            }
        }
    }
}

/// Get chat name from Peer
fn chat_name_from_peer(peer: &Peer) -> String {
    match peer {
        Peer::User(u) => u
            .first_name()
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("User {}", u.bare_id())),
        Peer::Group(g) => g
            .title()
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("Group {}", g.id().bare_id())),
        Peer::Channel(c) => c.title().to_string(),
    }
}

/// Get username from Peer if available
fn username_from_peer(peer: &Peer) -> Option<String> {
    match peer {
        Peer::User(u) => u.username().map(|s| s.to_string()),
        Peer::Channel(c) => c.username().map(|s| s.to_string()),
        Peer::Group(_) => None,
    }
}

/// Check if Peer is a forum
fn is_forum_peer(peer: &Peer) -> bool {
    match peer {
        Peer::Group(group) => {
            matches!(&group.raw, tl::enums::Chat::Channel(channel) if channel.forum)
        }
        Peer::Channel(channel) => channel.raw.forum,
        Peer::User(_) => false,
    }
}

/// Get access_hash from Peer
fn access_hash_from_peer(peer: &Peer) -> Option<i64> {
    match peer {
        Peer::User(u) => {
            // User.raw is tl::enums::User, need to match to get inner tl::types::User
            match &u.raw {
                tl::enums::User::User(user) => user.access_hash,
                tl::enums::User::Empty(_) => None,
            }
        }
        Peer::Channel(c) => c.raw.access_hash,
        Peer::Group(group) => match &group.raw {
            tl::enums::Chat::Channel(channel) => channel.access_hash,
            tl::enums::Chat::ChannelForbidden(channel) => Some(channel.access_hash),
            // Basic groups don't use access hashes.
            _ => None,
        },
    }
}

async fn enrich_message_peer(
    store: &Store,
    msg: &grammers_client::types::update::Message,
    peer: &Peer,
    chat_id: i64,
    ts: chrono::DateTime<Utc>,
    archived: bool,
) -> Result<()> {
    if let Some(Peer::User(user)) = msg.sender() {
        store
            .upsert_contact(
                user.bare_id(),
                user.username(),
                user.first_name().unwrap_or(""),
                user.last_name().unwrap_or(""),
                user.phone().unwrap_or(""),
            )
            .await?;
    }

    store
        .upsert_chat(
            chat_id,
            chat_kind_from_peer(peer),
            &chat_name_from_peer(peer),
            username_from_peer(peer).as_deref(),
            Some(ts),
            is_forum_peer(peer),
            access_hash_from_peer(peer),
            archived,
        )
        .await?;
    Ok(())
}

async fn persist_message_update(
    app: &App,
    args: &DaemonArgs,
    ignore_set: &HashSet<i64>,
    msg: grammers_client::types::update::Message,
    is_edit: bool,
    messages_stored: &AtomicU64,
) -> Result<()> {
    let peer_id = msg.peer_id();
    let chat_id = peer_id.bare_id();
    let peer = msg.peer().ok().cloned();
    let store = app.get_store().await?;
    let existing_chat = store.get_chat(chat_id).await?;
    let chat_kind = peer
        .as_ref()
        .map(|value| chat_kind_from_peer(value).to_string())
        .or_else(|| existing_chat.as_ref().map(|chat| chat.kind.clone()))
        .unwrap_or_else(|| chat_kind_from_peer_id(peer_id).to_string());

    if ignore_set.contains(&chat_id) || (args.ignore_channels && chat_kind == "channel") {
        return Ok(());
    }

    let sender_id = extract_sender_id(&msg);
    let from_me = msg.outgoing();
    let text = msg.text().to_string();
    let ts = msg.date();
    let edit_ts = is_edit.then(Utc::now);
    let reply_to_id = msg.reply_to_message_id().map(|id| id as i64);
    let topic_id = extract_topic_id_from_raw(&msg.raw);
    let media_type = msg.media().map(|_| "media".to_string());

    store
        .persist_live_message(
            UpsertMessageParams {
                id: msg.id() as i64,
                chat_id,
                sender_id,
                ts,
                edit_ts,
                from_me,
                text: text.clone(),
                media_type: media_type.clone(),
                media_path: None,
                reply_to_id,
                topic_id,
            },
            &chat_kind,
        )
        .await
        .with_context(|| {
            format!(
                "Failed to durably persist {} message {} in chat {}",
                if is_edit { "edited" } else { "new" },
                msg.id(),
                chat_id
            )
        })?;
    messages_stored.fetch_add(1, Ordering::Relaxed);

    // Emit only after the durable write succeeds.
    if args.stream {
        use std::io::Write;
        let obj = serde_json::json!({
            "type": if is_edit { "message_edited" } else { "new_message" },
            "chat_id": chat_id,
            "id": msg.id(),
            "sender_id": sender_id,
            "from_me": from_me,
            "ts": ts.to_rfc3339(),
            "edit_ts": edit_ts.map(|value| value.to_rfc3339()),
            "text": text,
            "topic_id": topic_id,
            "media_type": media_type,
        });
        println!("{}", serde_json::to_string(&obj).unwrap_or_default());
        let _ = std::io::stdout().flush();
    }

    if let Some(peer) = peer.as_ref() {
        let archived = existing_chat.map(|chat| chat.archived).unwrap_or(false);
        if let Err(error) = enrich_message_peer(&store, &msg, peer, chat_id, ts, archived).await {
            // The message is already durable and the resolution marker remains
            // queued, so metadata failure is retryable rather than lossy.
            log::warn!(
                "Message {} in chat {} persisted; peer enrichment queued after error: {}",
                msg.id(),
                chat_id,
                error
            );
        }
    } else {
        log::warn!(
            "Message {} in chat {} persisted using raw peer ID; peer enrichment queued",
            msg.id(),
            chat_id
        );
    }

    Ok(())
}

async fn reconcile_recent_history(app: &mut App, args: &DaemonArgs, phase: &str) -> Result<()> {
    let (dialogs_refreshed, pending_peers) = app
        .refresh_active_dialogs(&args.ignore_chat_ids, args.ignore_channels)
        .await?;
    let opts = crate::app::sync::SyncOptions {
        output: crate::app::sync::OutputMode::None,
        mark_read: false,
        download_media: false,
        ignore_chat_ids: args.ignore_chat_ids.clone(),
        ignore_channels: args.ignore_channels,
        show_progress: false,
        incremental: true,
        messages_per_chat: 50,
        concurrency: 4,
        chat_filter: None,
        prune_after: None,
        skip_archived: false,
        archived_only: false,
        active_since: Some(Utc::now() - chrono::Duration::days(STARTUP_CATCHUP_DAYS)),
    };
    let result = app.sync_msgs(opts).await?;
    log::info!(
        "{} reconciliation: {} dialogs / {} msgs / {} chats / {} peers pending",
        phase,
        dialogs_refreshed,
        result.messages_stored,
        result.chats_stored,
        pending_peers
    );
    if !args.quiet {
        eprintln!(
            "{} reconciliation complete: {} new messages across {} chats ({} peers pending)",
            phase, result.messages_stored, result.chats_stored, pending_peers
        );
    }
    Ok(())
}

pub async fn run(cli: &Cli, args: &DaemonArgs) -> Result<()> {
    let mut app = App::new(cli).await?;

    // Catch up on messages missed while the daemon was down. Runs once, here,
    // sequentially *before* the live update stream starts — so it never
    // contends with the live writer for the DB lock (unlike the old concurrent
    // background backfill). This fires on exactly the events that open a gap:
    // reboot, crash, or a hard reconnect that restarts the process.
    if !args.no_startup_catchup {
        if !args.quiet {
            eprintln!("Startup catch-up sync...");
        }
        match reconcile_recent_history(&mut app, args, "Startup").await {
            Ok(()) => {}
            Err(e) => {
                // Never fatal — fall through to live listening regardless.
                log::warn!("startup catch-up failed (continuing to live listen): {}", e);
                if !args.quiet {
                    eprintln!("Startup catch-up failed (continuing): {}", e);
                }
            }
        }
    }

    // Take ownership of the updates receiver
    let updates_rx = app
        .updates_rx
        .take()
        .context("Updates receiver not available")?;

    let ignore_set: HashSet<i64> = args.ignore_chat_ids.iter().copied().collect();

    // Get global shutdown controller
    let shutdown_ctrl = shutdown::global();

    // Counters for statistics
    let messages_received = Arc::new(AtomicU64::new(0));
    let messages_stored = Arc::new(AtomicU64::new(0));
    let backfill_running = Arc::new(AtomicBool::new(false));

    if !args.quiet {
        eprintln!("Daemon starting...");
        eprintln!("  Listening for real-time updates");
        if !args.no_backfill {
            eprintln!("  Background sync will start after connection established");
        }
    }

    // Start the update stream - this subscribes to updates immediately
    // catch_up: true means it will also fetch any missed updates since last session
    let mut update_stream = app.tg.client.stream_updates(
        updates_rx,
        UpdatesConfiguration {
            // Update-state catch-up is independent from the optional concurrent
            // history backfill. Keeping it enabled closes MTProto update gaps;
            // idempotent SQLite upserts absorb duplicates.
            catch_up: true,
            ..Default::default()
        },
    );

    // Spawn background sync task if backfill is enabled
    let backfill_handle = if !args.no_backfill {
        let cli_clone = cli.clone();
        let ignore_ids = args.ignore_chat_ids.clone();
        let ignore_chans = args.ignore_channels;
        let backfill_running_clone = Arc::clone(&backfill_running);
        let shutdown_ctrl_clone = shutdown_ctrl.clone();
        let quiet = args.quiet;

        Some(tokio::spawn(async move {
            // Small delay to ensure update listener is fully established
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                _ = shutdown_ctrl_clone.cancelled() => {
                    return Ok::<_, anyhow::Error>(());
                }
            }

            if shutdown_ctrl_clone.is_triggered() {
                return Ok::<_, anyhow::Error>(());
            }

            backfill_running_clone.store(true, Ordering::Relaxed);
            if !quiet {
                eprintln!("Background sync starting...");
            }

            // Create a separate App instance for backfill
            let mut backfill_app = App::new(&cli_clone).await?;

            let opts = crate::app::sync::SyncOptions {
                output: crate::app::sync::OutputMode::None,
                mark_read: false,
                download_media: false,
                ignore_chat_ids: ignore_ids,
                ignore_channels: ignore_chans,
                show_progress: !quiet,
                incremental: true,
                messages_per_chat: 50,
                concurrency: 4,
                chat_filter: None,
                prune_after: None,
                skip_archived: false,
                archived_only: false,
                active_since: None,
            };

            let result = backfill_app.sync(opts).await;
            backfill_running_clone.store(false, Ordering::Relaxed);

            match result {
                Ok(res) => {
                    if !quiet {
                        eprintln!(
                            "Background sync complete: {} chats, {} messages",
                            res.chats_stored, res.messages_stored
                        );
                    }
                }
                Err(e) => {
                    // Don't log error if we're shutting down
                    if !shutdown_ctrl_clone.is_triggered() {
                        log::error!("Background sync failed: {}", e);
                        if !quiet {
                            eprintln!("Background sync failed: {}", e);
                        }
                    }
                }
            }

            Ok(())
        }))
    } else {
        None
    };

    if !args.quiet {
        eprintln!("Daemon ready. Press Ctrl+C to stop.");
    }

    let reconciliation_enabled = args.reconcile_interval_seconds > 0;
    let mut reconciliation_interval =
        tokio::time::interval(Duration::from_secs(args.reconcile_interval_seconds.max(1)));
    reconciliation_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // The startup reconciliation above already covers the immediate tick.
    reconciliation_interval.tick().await;

    // Main update loop
    loop {
        tokio::select! {
            _ = shutdown_ctrl.cancelled() => {
                if !args.quiet {
                    eprintln!("\nShutting down gracefully...");
                }
                break;
            }
            _ = reconciliation_interval.tick(), if reconciliation_enabled && !backfill_running.load(Ordering::Relaxed) => {
                if !args.quiet {
                    eprintln!("Periodic reconciliation sync...");
                }
                if let Err(e) = reconcile_recent_history(&mut app, args, "Periodic").await {
                    log::warn!("periodic reconciliation failed (will retry): {}", e);
                    if !args.quiet {
                        eprintln!("Periodic reconciliation failed (will retry): {}", e);
                    }
                }
            }
            update_result = update_stream.next() => {
                match update_result {
                    Ok(update) => {
                        messages_received.fetch_add(1, Ordering::Relaxed);

                        match update {
                            Update::NewMessage(msg) => {
                                persist_message_update(
                                    &app,
                                    args,
                                    &ignore_set,
                                    msg,
                                    false,
                                    messages_stored.as_ref(),
                                )
                                .await?;
                            }
                            Update::MessageEdited(msg) => {
                                persist_message_update(
                                    &app,
                                    args,
                                    &ignore_set,
                                    msg,
                                    true,
                                    messages_stored.as_ref(),
                                )
                                .await?;
                            }
                            Update::MessageDeleted(deletion) => {
                                // Extract deleted message IDs from raw update
                                let (chat_id, msg_ids) = match &deletion.raw {
                                    tl::enums::Update::DeleteMessages(d) => {
                                        (None, d.messages.clone())
                                    }
                                    tl::enums::Update::DeleteChannelMessages(d) => {
                                        (Some(d.channel_id), d.messages.clone())
                                    }
                                    _ => continue,
                                };

                                if args.stream {
                                    use std::io::Write;
                                    let obj = serde_json::json!({
                                        "type": "message_deleted",
                                        "chat_id": chat_id,
                                        "message_ids": msg_ids,
                                    });
                                    println!("{}", serde_json::to_string(&obj).unwrap_or_default());
                                    let _ = std::io::stdout().flush();
                                }

                                // Note: We don't delete from local DB by default
                                // Messages remain for history. Add --delete-on-remote-delete flag if needed.
                            }
                            Update::Raw(raw) => {
                                // Log unhandled update types for debugging
                                log::debug!("Unhandled raw update: {:?}", raw.raw);
                            }
                            _ => {
                                // CallbackQuery, InlineQuery, etc. - not relevant for message sync
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Update stream error: {}", e);
                        if !args.quiet {
                            eprintln!("Update stream error: {}", e);
                        }
                        // For transient errors, continue. For fatal errors, break.
                        if e.to_string().contains("Dropped") {
                            break;
                        }
                    }
                }
            }
        }
    }

    // Wait for backfill to finish if running (with timeout)
    if let Some(handle) = backfill_handle {
        if backfill_running.load(Ordering::Relaxed) && !args.quiet {
            eprintln!("Waiting for background sync to complete...");
        }
        // Give backfill a chance to finish, but don't wait forever
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
    }

    // Sync update state to session before exit
    if !args.quiet {
        eprintln!("Syncing session state...");
    }
    update_stream.sync_update_state();

    if !args.quiet {
        eprintln!(
            "Daemon stopped. Updates received: {}, stored: {}",
            messages_received.load(Ordering::Relaxed),
            messages_stored.load(Ordering::Relaxed)
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_raw_peer_ids_without_resolving_metadata() {
        assert_eq!(chat_kind_from_peer_id(PeerId::user(42)), "user");
        assert_eq!(chat_kind_from_peer_id(PeerId::chat(42)), "group");
        assert_eq!(chat_kind_from_peer_id(PeerId::channel(42)), "channel");
    }

    #[test]
    fn extracts_bare_ids_from_raw_peers() {
        assert_eq!(
            bare_id_from_raw_peer(&tl::enums::Peer::User(tl::types::PeerUser { user_id: 11 })),
            11
        );
        assert_eq!(
            bare_id_from_raw_peer(&tl::enums::Peer::Chat(tl::types::PeerChat { chat_id: 22 })),
            22
        );
        assert_eq!(
            bare_id_from_raw_peer(&tl::enums::Peer::Channel(tl::types::PeerChannel {
                channel_id: 33,
            })),
            33
        );
    }
}
