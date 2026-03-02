use std::io::IsTerminal;
use std::sync::atomic::AtomicBool;

use crate::hooks::{self, HookContext};
use vykar_core::commands;
use vykar_core::compress::Compression;
use vykar_core::config::{self, SourceEntry, VykarConfig};

use crate::format::{format_bytes, generate_snapshot_name};
use crate::passphrase::with_repo_passphrase;
use crate::progress::BackupProgressRenderer;

fn run_backup_operation(
    config: &VykarConfig,
    req: commands::backup::BackupRequest<'_>,
    show_progress: bool,
    shutdown: Option<&AtomicBool>,
) -> Result<commands::backup::BackupOutcome, Box<dyn std::error::Error>> {
    if !show_progress {
        return commands::backup::run_with_progress(config, req, None, shutdown)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) });
    }

    let mut renderer = BackupProgressRenderer::new();
    let mut on_progress = |event| renderer.on_event(event);
    let result = commands::backup::run_with_progress(config, req, Some(&mut on_progress), shutdown);
    renderer.finish();

    result.map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
}

/// Returns `Ok(true)` if the backup completed with partial success (some files skipped),
/// `Ok(false)` for full success.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_backup(
    config: &VykarConfig,
    label: Option<&str>,
    user_label: Option<String>,
    compression_override: Option<String>,
    upload_concurrency: Option<usize>,
    paths: Vec<String>,
    sources: &[SourceEntry],
    source_filter: &[String],
    shutdown: Option<&AtomicBool>,
) -> Result<bool, Box<dyn std::error::Error>> {
    // Apply upload concurrency override before opening the repo
    let config = if let Some(uc) = upload_concurrency {
        let mut cfg = config.clone();
        cfg.limits.cpu.max_upload_concurrency = Some(uc);
        cfg
    } else {
        config.clone()
    };
    let config = &config;

    with_repo_passphrase(config, label, |passphrase| {
        let user_label_str = user_label.as_deref().unwrap_or("");
        let show_progress = std::io::stderr().is_terminal();

        // Determine compression
        let compression = if let Some(ref algo) = compression_override {
            Compression::from_config(algo, config.compression.zstd_level)?
        } else {
            Compression::from_algorithm(config.compression.algorithm, config.compression.zstd_level)
        };

        if !source_filter.is_empty() && !paths.is_empty() {
            return Err("cannot combine --source with ad-hoc paths".into());
        }

        if user_label.is_some() && paths.is_empty() {
            return Err("--label can only be used with ad-hoc paths".into());
        }

        let mut had_partial = false;

        if !paths.is_empty() {
            // Ad-hoc paths mode: group all paths into a single snapshot
            let expanded: Vec<String> = paths.iter().map(|p| config::expand_tilde(p)).collect();
            let source_label = if !user_label_str.is_empty() {
                user_label_str.to_string()
            } else if expanded.len() == 1 {
                config::label_from_path(&expanded[0])
            } else {
                "adhoc".to_string()
            };
            let name = generate_snapshot_name();

            let outcome = run_backup_operation(
                config,
                commands::backup::BackupRequest {
                    snapshot_name: &name,
                    passphrase,
                    source_paths: &expanded,
                    source_label: &source_label,
                    exclude_patterns: &config.exclude_patterns,
                    exclude_if_present: &config.exclude_if_present,
                    one_file_system: config.one_file_system,
                    git_ignore: config.git_ignore,
                    xattrs_enabled: config.xattrs.enabled,
                    compression,
                    command_dumps: &[],
                },
                show_progress,
                shutdown,
            )?;

            let stats = &outcome.stats;
            if outcome.is_partial {
                had_partial = true;
                eprintln!(
                    "Warning: {} file(s) could not be read and were excluded from the snapshot",
                    stats.errors
                );
            }

            println!("Snapshot created: {name}");
            let paths_display = expanded.join(", ");
            println!("  Source: {paths_display} (label: {source_label})");
            if stats.errors > 0 {
                println!(
                    "  Files: {}, Errors: {}, Original: {}, Compressed: {}, Deduplicated: {}",
                    stats.nfiles,
                    stats.errors,
                    format_bytes(stats.original_size),
                    format_bytes(stats.compressed_size),
                    format_bytes(stats.deduplicated_size),
                );
            } else {
                println!(
                    "  Files: {}, Original: {}, Compressed: {}, Deduplicated: {}",
                    stats.nfiles,
                    format_bytes(stats.original_size),
                    format_bytes(stats.compressed_size),
                    format_bytes(stats.deduplicated_size),
                );
            }
        } else if sources.is_empty() {
            return Err("no sources configured and no paths specified".into());
        } else {
            // Filter sources by --source if specified
            let active_sources: Vec<&SourceEntry> = if source_filter.is_empty() {
                sources.iter().collect()
            } else {
                config::select_sources(sources, source_filter)
                    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?
            };

            for source in &active_sources {
                let name = generate_snapshot_name();

                let has_source_hooks = !source.hooks.before.is_empty()
                    || !source.hooks.after.is_empty()
                    || !source.hooks.failed.is_empty()
                    || !source.hooks.finally.is_empty();

                let partial_flag = &mut had_partial;
                let mut backup_action = || -> Result<(), Box<dyn std::error::Error>> {
                    let outcome = run_backup_operation(
                        config,
                        commands::backup::BackupRequest {
                            snapshot_name: &name,
                            passphrase,
                            source_paths: &source.paths,
                            source_label: &source.label,
                            exclude_patterns: &source.exclude,
                            exclude_if_present: &source.exclude_if_present,
                            one_file_system: source.one_file_system,
                            git_ignore: source.git_ignore,
                            xattrs_enabled: source.xattrs_enabled,
                            compression,
                            command_dumps: &source.command_dumps,
                        },
                        show_progress,
                        shutdown,
                    )?;

                    let stats = &outcome.stats;
                    if outcome.is_partial {
                        *partial_flag = true;
                        eprintln!(
                            "Warning: {} file(s) could not be read and were excluded from the snapshot",
                            stats.errors
                        );
                    }

                    println!("Snapshot created: {name}");
                    let paths_display = source.paths.join(", ");
                    println!("  Source: {paths_display} (label: {})", source.label);
                    if stats.errors > 0 {
                        println!(
                            "  Files: {}, Errors: {}, Original: {}, Compressed: {}, Deduplicated: {}",
                            stats.nfiles,
                            stats.errors,
                            format_bytes(stats.original_size),
                            format_bytes(stats.compressed_size),
                            format_bytes(stats.deduplicated_size),
                        );
                    } else {
                        println!(
                            "  Files: {}, Original: {}, Compressed: {}, Deduplicated: {}",
                            stats.nfiles,
                            format_bytes(stats.original_size),
                            format_bytes(stats.compressed_size),
                            format_bytes(stats.deduplicated_size),
                        );
                    }
                    Ok(())
                };

                if has_source_hooks {
                    let mut ctx = HookContext {
                        command: "backup".to_string(),
                        repository: config.repository.url.clone(),
                        label: label.map(|s| s.to_string()),
                        error: None,
                        source_label: Some(source.label.clone()),
                        source_paths: Some(source.paths.clone()),
                    };
                    hooks::run_source_hooks(&source.hooks, &mut ctx, backup_action)?;
                } else {
                    backup_action()?;
                }
            }
        }

        Ok(had_partial)
    })
}
