use vykar_core::config::VykarConfig;

pub(crate) fn run_break_lock(
    config: &VykarConfig,
    _label: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = vykar_core::storage::backend_from_config(&config.repository)
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
    let removed = vykar_core::repo::lock::break_lock(storage.as_ref())
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

    if removed == 0 {
        println!("No locks found.");
    } else {
        println!("Removed {removed} lock(s).");
    }
    Ok(())
}
