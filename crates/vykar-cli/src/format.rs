use rand::RngCore;

/// Generate an 8-character hex snapshot name (4 random bytes).
pub(crate) fn generate_snapshot_name() -> String {
    let mut buf = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

pub(crate) fn parse_duration_span(
    s: &str,
) -> Result<chrono::DateTime<chrono::Utc>, Box<dyn std::error::Error>> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration string".into());
    }

    let (num_str, suffix) = match s.as_bytes().last() {
        Some(b'h' | b'H') => (&s[..s.len() - 1], 3600i64),
        Some(b'd' | b'D') => (&s[..s.len() - 1], 86400i64),
        Some(b'w' | b'W') => (&s[..s.len() - 1], 604800i64),
        _ => {
            return Err(format!(
                "invalid duration '{s}': use a suffix of h, d, or w (e.g. 24h, 7d, 2w)"
            )
            .into())
        }
    };

    let n: i64 = num_str
        .parse()
        .map_err(|_| format!("invalid duration number: '{num_str}'"))?;

    if n <= 0 {
        return Err(format!("--since duration must be positive (got '{s}')").into());
    }

    let seconds = n * suffix;
    let duration = chrono::Duration::seconds(seconds);
    Ok(chrono::Utc::now() - duration)
}

/// Parse a human-readable size string like "500M", "2G", "1024K" into bytes.
pub(crate) fn parse_size(s: &str) -> Result<u64, Box<dyn std::error::Error>> {
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
    Ok((num * multiplier as f64) as u64)
}

pub(crate) fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

pub(crate) fn format_size_with_bytes(bytes: u64) -> String {
    format_bytes(bytes)
}

pub(crate) fn format_size_with_savings(bytes: u64, reference: u64, label: &str) -> String {
    if reference == 0 {
        return format_bytes(bytes);
    }
    let pct = (1.0 - bytes as f64 / reference as f64) * 100.0;
    format!("{}  ({:.1}% {label})", format_bytes(bytes), pct)
}
