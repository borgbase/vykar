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

/// Parse a human-readable size string like "500M", "2G", "1024K" into bytes.
pub fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty size string".into());
    }

    let (num_str, multiplier) = match s.as_bytes().last() {
        Some(b'K' | b'k') => (&s[..s.len() - 1], 1024u64),
        Some(b'M' | b'm') => (&s[..s.len() - 1], 1024 * 1024),
        Some(b'G' | b'g') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        Some(b'T' | b't') => (&s[..s.len() - 1], 1024 * 1024 * 1024 * 1024),
        _ => (s, 1u64),
    };

    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid size: '{s}'"))?;
    if !num.is_finite() || num < 0.0 {
        return Err(format!("invalid size: '{s}'"));
    }
    let bytes = num * multiplier as f64;
    if bytes > u64::MAX as f64 {
        return Err(format!("size too large: '{s}'"));
    }
    Ok(bytes as u64)
}
