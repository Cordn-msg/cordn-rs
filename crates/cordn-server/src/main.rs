//! cordn-server bin entrypoint — builds the coordinator, wires the ContextVM
//! rmcp server over the Nostr transport, and runs until shutdown.
#![cfg(feature = "server")]

use std::sync::Arc;

use anyhow::{Context, Result};
use contextvm_sdk::transport::open_stream::OpenStreamConfig;
use contextvm_sdk::transport::server::{NostrServerTransport, NostrServerTransportConfig};
use contextvm_sdk::{signer, EncryptionMode, GiftWrapMode, ServerInfo};
use rmcp::ServiceExt;

use cordn_core::{
    default_now, Coordinator, CoordinatorOptions, InMemoryCoordinatorStorage,
    SqliteCoordinatorStorage, DEFAULT_CLEANUP_INTERVAL_MS,
};
use cordn_server::adapter::{CoordinatorAdapter, Now};
use cordn_server::config::{self, StorageBackend};
use cordn_server::methods::CordnServer;

fn build_transport_config(cfg: &config::ServerConfig) -> NostrServerTransportConfig {
    let mut server_info = ServerInfo::default()
        .with_name(cfg.server_name.clone())
        .with_about(cfg.server_about.clone().unwrap_or_default());
    // `website` is optional in TS (runtimeConfig) and the SDK field is
    // `Option<String>` with `skip_serializing_if = None`, so only set it when
    // configured — an empty string would serialize onto the announcement.
    if let Some(website) = &cfg.server_website {
        server_info = server_info.with_website(website.clone());
    }
    NostrServerTransportConfig::default()
        .with_relay_urls(cfg.relay_urls.clone())
        .with_announced_server(cfg.is_announced)
        .with_encryption_mode(EncryptionMode::Optional)
        .with_gift_wrap_mode(GiftWrapMode::Optional)
        .with_server_info(server_info)
        // CEP-22 oversized transfer (large key packages) is enabled by default;
        // CEP-41 open-stream (subscription tools) must be opted in.
        .with_open_stream(OpenStreamConfig::default().with_enabled(true))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // Include `nostr_relay_pool` so the relay connection lifecycle is
                // visible out of the box: nostr-sdk's connect() is non-blocking,
                // and it logs "Connected to '<url>'" / "Connection failed" under
                // this target. Without it the server silently connects in the
                // background with no visible confirmation.
                tracing_subscriber::EnvFilter::new(
                    "cordn_server=info,contextvm_sdk=info,nostr_relay_pool=info,nostr=info,rmcp=warn",
                )
            }),
        )
        .init();

    let cfg = config::load().context("reading cordn server config")?;

    let signer = match &cfg.private_key_hex {
        Some(hex) => signer::from_sk(hex).context("CORDN_SERVER_PRIVATE_KEY")?,
        None => signer::generate(),
    };
    let server_pubkey = signer.public_key().to_hex();

    let now: Now = default_now();
    let storage: Arc<dyn cordn_core::CoordinatorStorage> = match cfg.storage.backend {
        StorageBackend::Memory => Arc::new(InMemoryCoordinatorStorage::new()),
        StorageBackend::Sqlite => {
            let path = cfg
                .storage
                .sqlite_path
                .clone()
                .unwrap_or_else(|| "./cordn.sqlite".into());
            Arc::new(
                SqliteCoordinatorStorage::open(Some(&path), cfg.storage.sqlite_synchronous)
                    .with_context(|| format!("opening sqlite store at {path}"))?,
            )
        }
    };
    let coordinator = Coordinator::new(CoordinatorOptions {
        storage: Some(storage),
        now: Some(now.clone()),
        cleanup_interval_ms: Some(DEFAULT_CLEANUP_INTERVAL_MS),
        max_age_ms: Some(cfg.max_age_ms),
    });

    let adapter = Arc::new(CoordinatorAdapter::new(
        coordinator,
        cfg.abuse_protection.clone(),
        now,
    ));

    print_banner(&server_pubkey, &cfg.relay_urls);

    let transport = NostrServerTransport::new(signer, build_transport_config(&cfg))
        .await
        .context("connecting ContextVM server transport")?;

    tracing::info!(
        relays = ?cfg.relay_urls,
        announced = cfg.is_announced,
        server_pubkey = %server_pubkey,
        "ContextVM MLS coordinator server starting (relay connection completes in the background)"
    );

    let service = CordnServer::new(adapter).serve(transport).await?;

    // Run until SIGINT/SIGTERM.
    tokio::select! {
        result = service.waiting() => {
            if let Err(e) = result {
                tracing::error!(error = ?e, "server service exited with error");
            }
        }
        _ = shutdown_signal() => {
            tracing::info!("cordn server shutting down");
        }
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await;
    }
}

fn print_banner(server_pubkey: &str, relay_urls: &[String]) {
    let rule: String = "═".repeat(68);
    let relay_lines = if relay_urls.is_empty() {
        "     (none configured)".to_string()
    } else {
        relay_urls
            .iter()
            .map(|r| format!("     • {r}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    println!("\n  {rule}");
    println!("   🔑  CORDN COORDINATOR — Server Public Key");
    println!("  {rule}\n");
    println!("   pubkey    {server_pubkey}");
    println!("\n   📡  relays");
    println!("{relay_lines}");
    println!();
}
