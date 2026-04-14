#[derive(Debug, Clone)]
pub struct ServerSection {
    /// Address to listen on.
    pub listen: String,

    /// Root directory where repositories are stored.
    pub data_dir: String,

    /// Shared bearer token for authentication.
    pub token: String,

    /// If true, only index/index.gen/locks/sessions are overwritable; all other objects are immutable once written. DELETEs are restricted to locks/sessions.
    pub append_only: bool,

    /// Log output format: "json" or "pretty".
    pub log_format: String,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            listen: "localhost:8585".to_string(),
            data_dir: "/var/lib/vykar".to_string(),
            token: String::new(),
            append_only: false,
            log_format: "pretty".to_string(),
        }
    }
}

pub use vykar_common::display::parse_size;
