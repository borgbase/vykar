use vykar_core::config;

pub(crate) fn run_config_generate(dest: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let path = match dest {
        Some(d) => std::path::PathBuf::from(d),
        None => pick_config_location()?,
    };

    if path.exists() {
        return Err(format!("file already exists: {}", path.display()).into());
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    std::fs::write(&path, config::minimal_config_template())?;
    println!("Config written to: {}", path.display());
    println!("Edit it to set your repository path and source directories.");
    Ok(())
}

fn pick_config_location() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
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
            .map_err(|_| format!("invalid choice: '{input}'"))?;
        if n == 0 || n > search_paths.len() {
            return Err(format!("choice out of range: {n}").into());
        }
        n - 1
    };

    Ok(search_paths[selection].0.clone())
}
