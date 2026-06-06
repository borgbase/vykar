/// Returns a minimal YAML config template suitable for bootstrapping.
pub fn minimal_config_template() -> &'static str {
    r#"# vykar configuration file
# Minimal required configuration.
# Full reference: https://vykar.borgbase.com/configuration

# repositories:
#   - url: /path/to/repo

# sources:
#   - /path/to/source

# Windows paths: use single quotes or no quotes (double quotes break on backslashes)
#   - 'C:\Users\me\Documents'

# --- Common optional settings (uncomment as needed) ---

# encryption:
#   passphrase: "secret"
#
# retention:
#   keep_daily: 7
#   keep_weekly: 4
#
# compression:
#   algorithm: zstd
#   zstd_level: 3
#
# exclude_patterns:                  # Gitignore-style, relative to each source dir
#   - "*.tmp"
#   - ".cache/**"
#   - "/Downloads"                   # Leading / anchors to source root, not filesystem root
#   # NOTE: absolute paths like "/Users/jane/Movies/TV" do NOT work
#
# schedule:
#   enabled: true
#   every: "24h"
"#
}

#[cfg(test)]
mod tests {
    use super::super::document::RawConfig;
    use super::super::resolution::resolve_document;
    use super::*;

    #[test]
    fn test_minimal_template_is_valid_yaml() {
        let template = minimal_config_template();
        // Template is valid YAML (everything uncommented is still parseable).
        let parsed: Result<RawConfig, _> = serde_yaml::from_str(template);
        assert!(
            parsed.is_ok(),
            "template should parse as valid YAML: {:?}",
            parsed.err()
        );
        // With repositories commented out, resolve_document should return an empty vec.
        let raw = parsed.unwrap();
        let result = resolve_document(raw).unwrap();
        assert!(result.is_empty(), "expected empty vec for template config");
    }
}
