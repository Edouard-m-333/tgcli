//! Local, credential-free health and history state for the Northstar worker.
use crate::app::App;
use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

pub const CONTRACT: &str = "northstar-macos-v1";

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Health {
    pub contract: String,
    pub checked_at: String,
    pub connected_at: Option<String>,
    pub capture_checked_at: Option<String>,
    pub account_id: Option<String>,
    pub error: Option<String>,
}

pub fn write_private_json(path: &Path, value: &impl Serialize) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

pub async fn probe(app: &App, health: &mut Health) -> Result<()> {
    health.contract = CONTRACT.into();
    health.checked_at = Utc::now().to_rfc3339();
    match tokio::time::timeout(std::time::Duration::from_secs(15), app.tg.client.get_me()).await {
        Ok(Ok(me)) => {
            health.connected_at = Some(health.checked_at.clone());
            health.account_id = Some(me.bare_id().to_string());
            health.error = None;
        }
        _ => health.error = Some("Telegram connection needs attention".into()),
    }
    match app.get_store().await {
        Ok(store) => match store.count_messages().await {
            Ok(_) => health.capture_checked_at = Some(health.checked_at.clone()),
            Err(_) => health.error = Some("Local archive needs attention".into()),
        },
        Err(_) => health.error = Some("Local archive needs attention".into()),
    }
    write_private_json(
        &Path::new(&app.store_dir).join("northstar-health.json"),
        health,
    )
}

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryChat {
    pub complete: bool,
    pub messages_stored: usize,
    pub error: Option<String>,
}

#[derive(Default, Deserialize, Serialize)]
pub struct HistoryState {
    pub chats: BTreeMap<String, HistoryChat>,
    #[serde(default)]
    pub next_chat: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HistoryRequest {
    chat_ids: Vec<i64>,
}

/// Fetch one bounded page on the daemon's writer, never a competing process.
pub async fn history_page(app: &App) -> Result<()> {
    let root = Path::new(&app.store_dir);
    let request = match std::fs::read(root.join("northstar-history-request.json")) {
        Ok(bytes) => serde_json::from_slice::<HistoryRequest>(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(request.chat_ids.len() <= 10000, "Too many history requests");
    let path = root.join("northstar-history.json");
    let mut state: HistoryState = match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).context("Invalid history state")?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => HistoryState::default(),
        Err(error) => return Err(error.into()),
    };
    for offset in 0..request.chat_ids.len() {
        let index = (state.next_chat + offset) % request.chat_ids.len();
        let chat_id = request.chat_ids[index];
        let chat = state.chats.entry(chat_id.to_string()).or_default();
        if chat.complete {
            continue;
        }
        let oldest = app
            .get_store()
            .await?
            .get_oldest_message_id(chat_id, None)
            .await?;
        match tokio::time::timeout(
            std::time::Duration::from_secs(60),
            app.backfill_messages_with_progress(chat_id, None, oldest, 200, false, false, false),
        )
        .await
        {
            Ok(Ok(count)) => {
                chat.messages_stored += count;
                chat.complete = count < 200 && !crate::shutdown::global().is_triggered();
                chat.error = None;
            }
            _ => chat.error = Some("History import paused; it will retry".into()),
        }
        state.next_chat = (index + 1) % request.chat_ids.len();
        write_private_json(&path, &state)?;
        break;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    #[test]
    fn state_is_private_and_survives_atomic_replacement() -> Result<()> {
        let directory =
            std::env::temp_dir().join(format!("tgcli-health-{}", rand::random::<u64>()));
        std::fs::create_dir(&directory)?;
        let path = directory.join("health.json");
        write_private_json(&path, &serde_json::json!({"healthy":true}))?;
        write_private_json(&path, &serde_json::json!({"healthy":false}))?;
        assert_eq!(
            std::fs::metadata(&path)?.permissions().mode() & 0o777,
            0o600
        );
        let value: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
        assert_eq!(value["healthy"], false);
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }
}
