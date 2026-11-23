#[derive(Debug, Clone)]
pub struct ServerSection {
    /// Address to listen on.
    pub listen: String,

    /// Root directory where repositories are stored.
    pub data_dir: String,

    /// Shared bearer token for authentication.
    pub token: String,

    /// If true, committed objects are neither overwritten nor deleted: only
    /// `locks/`, `sessions/` and the legacy `index` and `index.gen` stay
    /// mutable, and `index.d/` objects may be re-PUT with their own bytes.
    /// Only temp files, locks and sessions can be deleted.
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
