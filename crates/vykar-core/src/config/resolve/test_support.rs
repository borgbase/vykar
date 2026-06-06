use super::super::defaults::*;
use super::super::hooks::{HooksConfig, SourceHooksConfig};
use super::super::limits::ResourceLimitsConfig;
use super::super::sources::SourceEntry;
use super::super::types::*;
use super::resolution::ResolvedRepo;

/// RAII guard to set an env var and restore its previous value on drop.
pub(super) struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    pub(super) fn set(key: &'static str, val: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, val);
        Self { key, prev }
    }

    pub(super) fn unset(key: &'static str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::remove_var(key);
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

pub(super) fn make_test_repo(url: &str, label: Option<&str>) -> ResolvedRepo {
    ResolvedRepo {
        label: label.map(|s| s.to_string()),
        config: VykarConfig {
            repository: RepositoryConfig {
                url: url.to_string(),
                region: None,
                access_key_id: None,
                secret_access_key: None,
                sftp_key: None,
                sftp_known_hosts: None,
                sftp_timeout: None,
                access_token: None,
                allow_insecure_http: false,
                min_pack_size: default_min_pack_size(),
                max_pack_size: default_max_pack_size(),
                retry: RetryConfig::default(),
                s3_soft_delete: false,
            },
            encryption: EncryptionConfig::default(),
            exclude_patterns: vec![],
            exclude_if_present: vec![],
            one_file_system: default_one_file_system(),
            git_ignore: false,
            chunker: ChunkerConfig::default(),
            compression: CompressionConfig::default(),
            retention: RetentionConfig::default(),
            xattrs: XattrsConfig::default(),
            schedule: ScheduleConfig::default(),
            limits: ResourceLimitsConfig::default(),
            compact: CompactConfig::default(),
            check: CheckConfig::default(),
            cache_dir: None,
            trust_repo: false,
            hostname_override: None,
        },
        global_hooks: HooksConfig::default(),
        repo_hooks: HooksConfig::default(),
        sources: vec![],
    }
}

pub(super) fn make_test_source(label: &str) -> SourceEntry {
    SourceEntry {
        paths: vec![format!("/home/{label}")],
        label: label.to_string(),
        exclude: Vec::new(),
        exclude_if_present: Vec::new(),
        one_file_system: default_one_file_system(),
        git_ignore: false,
        xattrs_enabled: default_xattrs_enabled(),
        hooks: SourceHooksConfig::default(),
        retention: None,
        repos: Vec::new(),
        command_dumps: Vec::new(),
    }
}
