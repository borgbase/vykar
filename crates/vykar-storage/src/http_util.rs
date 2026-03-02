use vykar_types::error::{Result, VykarError};

/// Extract and parse the `Content-Length` header from an HTTP response.
pub fn extract_content_length(resp: &ureq::Response, context: &str) -> Result<u64> {
    let header = resp.header("Content-Length").ok_or_else(|| {
        VykarError::Other(format!("{context}: response missing Content-Length header"))
    })?;
    header.parse::<u64>().map_err(|_| {
        VykarError::Other(format!(
            "{context}: invalid Content-Length header: {header}"
        ))
    })
}
