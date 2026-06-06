pub fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    if bytes >= GIB {
        format_unit(bytes, GIB, "GiB")
    } else if bytes >= MIB {
        format_unit(bytes, MIB, "MiB")
    } else if bytes >= KIB {
        format_unit(bytes, KIB, "KiB")
    } else {
        format!("{bytes} B")
    }
}

fn format_unit(bytes: u64, unit: u64, suffix: &str) -> String {
    let whole = bytes / unit;
    let frac = ((bytes % unit) * 100 + unit / 2) / unit;
    if frac == 100 {
        format!("{}.00 {suffix}", whole + 1)
    } else {
        format!("{whole}.{frac:02} {suffix}")
    }
}

pub fn format_count(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(c);
    }
    result
}

/// Parse a human-readable size string like "500M", "2G", "1024K" into bytes.
///
/// Accepted grammar: `<digits>[.<digits>][KMGTkmgt]`. The optional suffix is
/// a binary multiplier (K = 1024, M = 1024², G = 1024³, T = 1024⁴). Plain
/// digits with no suffix are interpreted as bytes.
///
/// Scientific notation (e.g. `"1e9"`) is **not** supported; use `"1G"` or
/// `"1024M"` instead. The parser is integer-based and exact across the full
/// `u64` range — fractional values are computed without floating point.
///
/// # Errors
///
/// Returns an error when the string is empty, uses an invalid numeric form, is
/// negative, or exceeds `u64::MAX` bytes after applying the suffix multiplier.
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

    // Try integer parse first (exact for the full u64 range), fall back to
    // f64 only for fractional values like "1.5G".
    if let Ok(n) = num_str.parse::<u64>() {
        return n
            .checked_mul(multiplier)
            .ok_or_else(|| format!("size too large: '{s}'"));
    }

    parse_decimal_size(num_str, multiplier).map_err(|kind| match kind {
        DecimalSizeError::Invalid => format!("invalid size: '{s}'"),
        DecimalSizeError::TooLarge => format!("size too large: '{s}'"),
    })
}

enum DecimalSizeError {
    Invalid,
    TooLarge,
}

fn parse_decimal_size(num_str: &str, multiplier: u64) -> Result<u64, DecimalSizeError> {
    let (whole_str, frac_str) = num_str.split_once('.').ok_or(DecimalSizeError::Invalid)?;
    if whole_str.starts_with('-') || frac_str.starts_with('-') {
        return Err(DecimalSizeError::Invalid);
    }
    if whole_str.is_empty() && frac_str.is_empty() {
        return Err(DecimalSizeError::Invalid);
    }
    if !whole_str.chars().all(|c| c.is_ascii_digit())
        || !frac_str.chars().all(|c| c.is_ascii_digit())
    {
        return Err(DecimalSizeError::Invalid);
    }

    let whole = if whole_str.is_empty() {
        0
    } else {
        whole_str
            .parse::<u128>()
            .map_err(|_| DecimalSizeError::TooLarge)?
    };

    let multiplier = u128::from(multiplier);
    let whole_bytes = whole
        .checked_mul(multiplier)
        .ok_or(DecimalSizeError::TooLarge)?;

    let mut frac_value = 0u128;
    let mut frac_scale = 1u128;
    for digit in frac_str.bytes() {
        frac_value = frac_value
            .checked_mul(10)
            .and_then(|v| v.checked_add(u128::from(digit - b'0')))
            .ok_or(DecimalSizeError::TooLarge)?;
        frac_scale = frac_scale
            .checked_mul(10)
            .ok_or(DecimalSizeError::TooLarge)?;
    }
    let frac_bytes = frac_value
        .checked_mul(multiplier)
        .ok_or(DecimalSizeError::TooLarge)?
        / frac_scale;

    let bytes = whole_bytes
        .checked_add(frac_bytes)
        .ok_or(DecimalSizeError::TooLarge)?;
    u64::try_from(bytes).map_err(|_| DecimalSizeError::TooLarge)
}

/// Return the terminal display width of a single character.
/// CJK and fullwidth characters occupy 2 columns; everything else occupies 1.
pub fn char_display_width(c: char) -> usize {
    let cp = c as u32;
    if matches!(cp,
        0x1100..=0x115F   // Hangul Jamo initials
        | 0x2E80..=0x303E // CJK Radicals, Kangxi, Symbols & Punctuation
        | 0x3040..=0x33FF // Hiragana, Katakana, Bopomofo, CJK Compat
        | 0x3400..=0x4DBF // CJK Extension A
        | 0x4E00..=0x9FFF // CJK Unified Ideographs
        | 0xAC00..=0xD7AF // Hangul Syllables
        | 0xF900..=0xFAFF // CJK Compat Ideographs
        | 0xFE30..=0xFE6F // CJK Compat Forms
        | 0xFF01..=0xFF60 // Fullwidth Forms
        | 0xFFE0..=0xFFE6 // Fullwidth Signs
        | 0x20000..=0x3FFFF // CJK Extensions B–G
    ) {
        2
    } else {
        1
    }
}

/// Return the terminal display width of a string (sum of character widths).
pub fn str_display_width(s: &str) -> usize {
    s.chars().map(char_display_width).sum()
}

/// Truncate a string to `max_cols` **display columns**, showing both the
/// beginning and end with `...` in the middle (e.g. `/very/l...file.txt`).
pub fn truncate_middle(input: &str, max_cols: usize) -> String {
    if max_cols == 0 {
        return String::new();
    }

    let input_width = str_display_width(input);
    if input_width <= max_cols {
        return input.to_string();
    }

    if max_cols <= 3 {
        return ".".repeat(max_cols);
    }

    let keep = max_cols - 3; // columns available for head + tail
    let head_budget = keep / 2;
    let tail_budget = keep - head_budget;

    // Build head: take chars until we'd exceed head_budget columns.
    let mut head_str = String::new();
    let mut head_used = 0;
    for c in input.chars() {
        let w = char_display_width(c);
        if head_used + w > head_budget {
            break;
        }
        head_str.push(c);
        head_used += w;
    }

    // Build tail: take chars from the end until we'd exceed tail_budget columns.
    let mut tail_chars: Vec<char> = Vec::new();
    let mut tail_used = 0;
    for c in input.chars().rev() {
        let w = char_display_width(c);
        if tail_used + w > tail_budget {
            break;
        }
        tail_chars.push(c);
        tail_used += w;
    }
    tail_chars.reverse();
    let tail_str: String = tail_chars.into_iter().collect();

    format!("{head_str}...{tail_str}")
}

#[cfg(test)]
mod tests {
    use super::{parse_size, str_display_width, truncate_middle};

    #[test]
    fn truncate_middle_shows_head_and_tail() {
        let input = "/very/long/path/to/a/file.txt";
        let out = truncate_middle(input, 16);
        assert_eq!(out, "/very/...ile.txt");
        assert_eq!(str_display_width(&out), 16);
    }

    #[test]
    fn truncate_middle_returns_original_when_short() {
        assert_eq!(truncate_middle("short.txt", 32), "short.txt");
    }

    #[test]
    fn truncate_middle_handles_tiny_widths() {
        assert_eq!(truncate_middle("abcdef", 0), "");
        assert_eq!(truncate_middle("abcdef", 1), ".");
        assert_eq!(truncate_middle("abcdef", 2), "..");
        assert_eq!(truncate_middle("abcdef", 3), "...");
    }

    #[test]
    fn truncate_middle_exact_fit() {
        assert_eq!(truncate_middle("exactly10!", 10), "exactly10!");
    }

    #[test]
    fn truncate_middle_one_over() {
        let out = truncate_middle("abcdefghijk", 10);
        assert_eq!(out, "abc...hijk");
        assert_eq!(str_display_width(&out), 10);
    }

    #[test]
    fn truncate_middle_unicode() {
        let input = "aaaa\u{00e9}\u{00e9}\u{00e9}\u{00e9}bbbb"; // 12 chars, all width 1
        let out = truncate_middle(input, 10);
        assert_eq!(out, "aaa...bbbb");
        assert_eq!(str_display_width(&out), 10);
    }

    #[test]
    fn truncate_middle_cjk() {
        let input = "文件/路径/测试报告.pdf";
        let out = truncate_middle(input, 16);
        assert_eq!(out, "文件/...告.pdf");
        assert!(str_display_width(&out) <= 16);
    }

    #[test]
    fn truncate_middle_combining_diaeresis() {
        // NFC-decomposed ö = o + \u{0308}
        let input = "Documents/Children Books/Das Lo\u{0308}schflugzeug Nummer 292/Das Lo\u{0308}schflugzeug Nummer 292-EN.ai";
        let out = truncate_middle(input, 60);
        assert!(str_display_width(&out) <= 60);
        assert!(out.contains("..."));
    }

    #[test]
    fn parse_size_basic() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("1k").unwrap(), 1024);
        assert_eq!(parse_size("2M").unwrap(), 2 * 1024 * 1024);
        assert_eq!(parse_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("1T").unwrap(), 1024u64 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_size_fractional() {
        assert_eq!(parse_size("1.5G").unwrap(), 3 * 1024 * 1024 * 1024 / 2);
    }

    #[test]
    fn parse_size_rejects_invalid() {
        assert!(parse_size("").is_err());
        assert!(parse_size("abc").is_err());
        assert!(parse_size("-1M").is_err());
        assert!(parse_size("NaN").is_err());
        assert!(parse_size("infG").is_err());
    }

    #[test]
    fn parse_size_rejects_scientific_notation() {
        // Scientific notation is intentionally unsupported — use "1G" / "1024M" instead.
        assert!(parse_size("1e9").is_err());
        assert!(parse_size("1.5e3").is_err());
        assert!(parse_size("1e9G").is_err());
    }

    #[test]
    fn parse_size_rejects_overflow() {
        // u64::MAX + 1 — must not silently saturate to u64::MAX
        assert!(parse_size("18446744073709551616").is_err());
        // Also reject when multiplied
        assert!(parse_size("17592186044416T").is_err()); // 16 EiB
    }

    #[test]
    fn parse_size_accepts_u64_max() {
        assert_eq!(parse_size("18446744073709551615").unwrap(), u64::MAX);
    }
}
