//! Runtime configuration parsed from environment variables / `.env` files —
//! port of `references/cordn/src/server/runtimeConfig.ts`. Produces plain
//! values; `main.rs` turns them into a signer, transport, and coordinator.

use cordn_core::Synchronous;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend {
    Memory,
    Sqlite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub sqlite_path: Option<String>,
    /// SQLite `synchronous` durability pragma. Ignored by the `memory` backend.
    /// `Normal` (default) is ~30–40× faster than `Full`; see `Synchronous`.
    pub sqlite_synchronous: Synchronous,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateLimitConfig {
    pub enabled: bool,
    pub refill_per_minute: i64,
    pub burst: i64,
    pub idle_ttl_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyPackageQuotaConfig {
    pub max_per_identity: usize,
    pub max_last_resort_per_identity: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AbuseProtectionConfig {
    pub rate_limit: RateLimitConfig,
    pub key_package_quota: KeyPackageQuotaConfig,
    pub log_rejections: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerConfig {
    /// Hex private key. `None` means generate an ephemeral key.
    pub private_key_hex: Option<String>,
    pub relay_urls: Vec<String>,
    pub server_name: String,
    pub server_about: Option<String>,
    pub server_website: Option<String>,
    pub is_announced: bool,
    pub storage: StorageConfig,
    pub abuse_protection: AbuseProtectionConfig,
    /// Max age in ms for welcome/join-request cleanup. `0` keeps forever.
    pub max_age_ms: i64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            private_key_hex: None,
            relay_urls: default_relay_urls(),
            server_name: "cordn-server".into(),
            server_about: None,
            server_website: None,
            is_announced: false,
            storage: StorageConfig {
                backend: StorageBackend::Memory,
                sqlite_path: None,
                sqlite_synchronous: Synchronous::Normal,
            },
            abuse_protection: AbuseProtectionConfig {
                rate_limit: RateLimitConfig {
                    enabled: true,
                    refill_per_minute: 500,
                    burst: 160,
                    idle_ttl_ms: 3_600_000,
                },
                key_package_quota: KeyPackageQuotaConfig {
                    max_per_identity: 50,
                    max_last_resort_per_identity: 1,
                },
                log_rejections: true,
            },
            max_age_ms: 30 * 86_400_000,
        }
    }
}

pub fn default_relay_urls() -> Vec<String> {
    vec!["wss://relay.contextvm.org".into()]
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Invalid boolean environment variable: {0}")]
    InvalidBoolean(String),
    #[error("Invalid integer environment variable: {0}")]
    InvalidInteger(String),
    #[error("Invalid storage backend in CORDN_STORAGE_BACKEND: expected 'memory' or 'sqlite'")]
    InvalidStorageBackend,
    #[error("Invalid CORDN_SQLITE_SYNCHRONOUS: expected 'normal' or 'full'")]
    InvalidSqliteSynchronous,
}

/// Load `.env` then `.env.local` into the process environment, without
/// overwriting variables that are already set. Missing files are ignored.
/// Ports `loadRuntimeEnv` / `loadEnvFile` from the TS reference.
pub fn load_env_files() {
    load_env_file(".env");
    load_env_file(".env.local");
}

fn load_env_file(path: &str) {
    let Ok(content) = std::fs::read_to_string(path) else {
        return;
    };
    for line in content.lines() {
        let Some((key, value)) = parse_env_assignment(line) else {
            continue;
        };
        if std::env::var_os(&key).is_none() {
            std::env::set_var(&key, &value);
        }
    }
}

fn parse_env_assignment(line: &str) -> Option<(String, String)> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    let normalized = trimmed
        .strip_prefix("export ")
        .map(str::trim)
        .unwrap_or(trimmed);
    let sep = normalized.find('=')?;
    if sep == 0 {
        return None;
    }
    let key = normalized[..sep].trim();
    if !key
        .chars()
        .next()
        .map(|c| c.is_ascii_alphabetic() || c == '_')
        .unwrap_or(false)
        || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return None;
    }
    let mut value = normalized[sep + 1..].trim().to_owned();
    if (value.starts_with('"') && value.ends_with('"') && value.len() >= 2)
        || (value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2)
    {
        value = value[1..value.len() - 1].to_owned();
    }
    Some((key.to_owned(), value))
}

fn opt_string(env: &std::collections::HashMap<String, String>, name: &str) -> Option<String> {
    env.get(name)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn opt_bool(
    env: &std::collections::HashMap<String, String>,
    name: &str,
) -> Result<Option<bool>, ConfigError> {
    match opt_string(env, name).as_deref() {
        None => Ok(None),
        Some("true") | Some("1") => Ok(Some(true)),
        Some("false") | Some("0") => Ok(Some(false)),
        Some(_) => Err(ConfigError::InvalidBoolean(name.into())),
    }
}

fn pos_int(
    env: &std::collections::HashMap<String, String>,
    name: &str,
    default: i64,
) -> Result<i64, ConfigError> {
    match opt_string(env, name).as_deref() {
        None => Ok(default),
        Some(raw) => raw
            .parse::<i64>()
            .ok()
            .filter(|v| *v >= 0)
            .ok_or_else(|| ConfigError::InvalidInteger(name.into())),
    }
}

/// Read the server config from the given environment map (defaults applied for
/// missing vars). Pass `std::env::vars().collect()` for the live environment.
pub fn read_server_config(
    env: &std::collections::HashMap<String, String>,
) -> Result<ServerConfig, ConfigError> {
    let private_key_hex = opt_string(env, "CORDN_SERVER_PRIVATE_KEY");

    let relay_urls = match opt_string(env, "CORDN_RELAY_URLS") {
        Some(raw) => {
            let urls: Vec<String> = raw
                .split(',')
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
                .collect();
            if urls.is_empty() {
                default_relay_urls()
            } else {
                urls
            }
        }
        None => default_relay_urls(),
    };

    let server_name = opt_string(env, "CORDN_SERVER_NAME").unwrap_or_else(|| "cordn-server".into());
    let server_about = opt_string(env, "CORDN_SERVER_ABOUT");
    let server_website = opt_string(env, "CORDN_SERVER_WEBSITE");
    let is_announced = opt_bool(env, "CORDN_ANNOUNCED")?.unwrap_or(false);

    let backend = opt_string(env, "CORDN_STORAGE_BACKEND").unwrap_or_else(|| "memory".into());
    let storage = match backend.as_str() {
        "memory" => StorageConfig {
            backend: StorageBackend::Memory,
            sqlite_path: None,
            sqlite_synchronous: Synchronous::Normal,
        },
        "sqlite" => {
            let sqlite_synchronous = match opt_string(env, "CORDN_SQLITE_SYNCHRONOUS") {
                None => Synchronous::Normal,
                Some(raw) => {
                    Synchronous::from_config(&raw).ok_or(ConfigError::InvalidSqliteSynchronous)?
                }
            };
            StorageConfig {
                backend: StorageBackend::Sqlite,
                sqlite_path: Some(
                    opt_string(env, "CORDN_SQLITE_PATH").unwrap_or_else(|| "./cordn.sqlite".into()),
                ),
                sqlite_synchronous,
            }
        }
        _ => return Err(ConfigError::InvalidStorageBackend),
    };

    let rate_limit = RateLimitConfig {
        enabled: opt_bool(env, "CORDN_RATE_LIMIT_ENABLED")?.unwrap_or(true),
        refill_per_minute: pos_int(env, "CORDN_RATE_LIMIT_REFILL_PER_MINUTE", 500)?,
        burst: pos_int(env, "CORDN_RATE_LIMIT_BURST", 160)?,
        idle_ttl_ms: pos_int(env, "CORDN_RATE_LIMIT_IDLE_TTL_SECONDS", 3600)? * 1000,
    };
    let key_package_quota = KeyPackageQuotaConfig {
        max_per_identity: pos_int(env, "CORDN_MAX_KEY_PACKAGES_PER_IDENTITY", 50)? as usize,
        max_last_resort_per_identity: pos_int(
            env,
            "CORDN_MAX_LAST_RESORT_KEY_PACKAGES_PER_IDENTITY",
            1,
        )? as usize,
    };
    let abuse_protection = AbuseProtectionConfig {
        rate_limit,
        key_package_quota,
        log_rejections: opt_bool(env, "CORDN_LOG_ABUSE_REJECTIONS")?.unwrap_or(true),
    };
    let max_age_ms = pos_int(env, "CORDN_MAX_AGE_DAYS", 30)? * 86_400_000;

    Ok(ServerConfig {
        private_key_hex,
        relay_urls,
        server_name,
        server_about,
        server_website,
        is_announced,
        storage,
        abuse_protection,
        max_age_ms,
    })
}

/// Convenience: load `.env` files then read from the live environment.
pub fn load() -> Result<ServerConfig, ConfigError> {
    load_env_files();
    read_server_config(&std::env::vars().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn defaults_match_ts() {
        let c = read_server_config(&HashMap::new()).unwrap();
        assert_eq!(c.relay_urls, vec!["wss://relay.contextvm.org"]);
        assert_eq!(c.server_name, "cordn-server");
        assert!(!c.is_announced);
        assert_eq!(c.storage.backend, StorageBackend::Memory);
        assert_eq!(c.storage.sqlite_synchronous, Synchronous::Normal);
        assert_eq!(c.abuse_protection.rate_limit.refill_per_minute, 500);
        assert_eq!(c.abuse_protection.rate_limit.burst, 160);
        assert_eq!(c.abuse_protection.rate_limit.idle_ttl_ms, 3_600_000);
        assert_eq!(c.abuse_protection.key_package_quota.max_per_identity, 50);
        assert_eq!(c.max_age_ms, 30 * 86_400_000);
    }

    #[test]
    fn parses_overrides() {
        let c = read_server_config(&env(&[
            ("CORDN_RELAY_URLS", "wss://a.test, wss://b.test"),
            ("CORDN_STORAGE_BACKEND", "sqlite"),
            ("CORDN_SQLITE_PATH", "/tmp/x.sqlite"),
            ("CORDN_SQLITE_SYNCHRONOUS", "full"),
            ("CORDN_RATE_LIMIT_ENABLED", "false"),
            ("CORDN_MAX_KEY_PACKAGES_PER_IDENTITY", "7"),
            ("CORDN_MAX_AGE_DAYS", "1"),
            ("CORDN_ANNOUNCED", "1"),
        ]))
        .unwrap();
        assert_eq!(c.relay_urls, vec!["wss://a.test", "wss://b.test"]);
        assert_eq!(c.storage.backend, StorageBackend::Sqlite);
        assert_eq!(c.storage.sqlite_path.as_deref(), Some("/tmp/x.sqlite"));
        assert_eq!(c.storage.sqlite_synchronous, Synchronous::Full);
        assert!(!c.abuse_protection.rate_limit.enabled);
        assert_eq!(c.abuse_protection.key_package_quota.max_per_identity, 7);
        assert_eq!(c.max_age_ms, 86_400_000);
        assert!(c.is_announced);
    }

    #[test]
    fn rejects_bad_backend() {
        let e = env(&[("CORDN_STORAGE_BACKEND", "redis")]);
        assert!(read_server_config(&e).is_err());
    }

    #[test]
    fn rejects_bad_synchronous() {
        let e = env(&[
            ("CORDN_STORAGE_BACKEND", "sqlite"),
            ("CORDN_SQLITE_SYNCHRONOUS", "turbo"),
        ]);
        assert!(matches!(
            read_server_config(&e),
            Err(ConfigError::InvalidSqliteSynchronous)
        ));
    }

    #[test]
    fn env_assignment_parser() {
        assert_eq!(
            parse_env_assignment("FOO=bar"),
            Some(("FOO".into(), "bar".into()))
        );
        assert_eq!(
            parse_env_assignment("export BAZ = \"hi\""),
            Some(("BAZ".into(), "hi".into()))
        );
        assert_eq!(parse_env_assignment("# comment"), None);
        assert_eq!(parse_env_assignment(""), None);
        assert_eq!(parse_env_assignment("1BAD=x"), None);
    }

    #[test]
    fn load_env_file_does_not_overwrite() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("cordn_env_test_{}.env", std::process::id()));
        let path_str = path.to_str().unwrap();
        std::fs::write(&path, "CORDN_TEST_KEEP=fromfile\n").unwrap();
        std::env::remove_var("CORDN_TEST_KEEP");
        // Use the file-scoped loader via load_env_file by calling it indirectly:
        // set a pre-existing var to confirm it is preserved.
        std::env::set_var("CORDN_TEST_KEEP", "preset");
        load_env_file(path_str);
        assert_eq!(std::env::var("CORDN_TEST_KEEP").unwrap(), "preset");
        let _ = std::fs::remove_file(path_str);
    }
}
