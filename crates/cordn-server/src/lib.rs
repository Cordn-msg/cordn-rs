//! `cordn-server` — ContextVM/Nostr rmcp server binding the cordn coordinator
//! core to the wire. The rmcp-free adapter logic lives in [`adapter`]; runtime
//! config in [`config`]; the rmcp tool glue + bin entrypoint are added alongside.

pub mod adapter;
pub mod config;

#[cfg(feature = "server")]
pub mod methods;

#[cfg(feature = "server")]
pub use methods::CordnServer;

pub use adapter::{AdapterError, CoordinatorAdapter, MessageSink, Now};
pub use config::{load as load_config, read_server_config, AbuseProtectionConfig, ServerConfig};
