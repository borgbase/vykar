use std::io::Write;

use vykar_core::config;

use crate::error::{CliError, CliResult};

pub(crate) fn run_config_generate(dest: Option<&str>) -> CliResult<()> {
    let path = match dest {
        Some(d) => std::path::PathBuf::from(d),
        None => pick_config_location()?,
    };

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    if let Err(err) = write_config_file(&path) {
        if err.kind() == std::io::ErrorKind::AlreadyExists {
            return Err(CliError::from(format!(
                "file already exists: {}",
                path.display()
            )));
        }
        return Err(err.into());
    }

    println!("Config written to: {}", path.display());
    println!("Edit it to set your repository path and source directories.");
    Ok(())
}

fn write_config_file(path: &std::path::Path) -> std::io::Result<()> {
    let mut file = new_config_file(path)?;
    file.write_all(config::minimal_config_template().as_bytes())?;
    file.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn new_config_file(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn new_config_file(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
}

fn pick_config_location() -> CliResult<std::path::PathBuf> {
    let search_paths = config::default_config_search_paths();

    let descriptions: &[&str] = &[
        "Best for: project-specific backups, version-controlled settings",
        "Best for: personal backups of your home directory",
        "Best for: server backups, runs as root or via systemd",
    ];

    let labels: &[&str] = &["Local directory", "User config", "System-wide"];

    eprintln!("Where should the config file live?");
    for (i, (((path, _level), label), desc)) in search_paths
        .iter()
        .zip(labels.iter())
        .zip(descriptions.iter())
        .enumerate()
    {
        eprintln!("  [{}] {} {}", i + 1, label, path.display());
        eprintln!("      {desc}");
    }
    eprint!("Choice [1]: ");
    std::io::Write::flush(&mut std::io::stderr())?;

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let input = input.trim();

    let selection = if input.is_empty() {
        0
    } else {
        let n: usize = input
            .parse()
            .map_err(|_| CliError::from(format!("invalid choice: '{input}'")))?;
        if n == 0 || n > search_paths.len() {
            return Err(CliError::from(format!("choice out of range: {n}")));
        }
        n - 1
    };

    Ok(search_paths
        .get(selection)
        .expect("selection is bounded above by search_paths.len()")
        .0
        .clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_template_at_requested_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vykar.yaml");

        run_config_generate(Some(path.to_str().unwrap())).unwrap();

        let written = std::fs::read_to_string(&path).unwrap();
        assert_eq!(written, config::minimal_config_template());
    }

    #[cfg(unix)]
    #[test]
    fn generated_config_is_not_group_or_world_readable() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vykar.yaml");

        run_config_generate(Some(path.to_str().unwrap())).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode & 0o077,
            0,
            "expected no group/world access, got {mode:o}"
        );
    }
}
