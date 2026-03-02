use std::path::PathBuf;

fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

#[cfg(unix)]
pub fn home_dir() -> Option<PathBuf> {
    env_path("HOME").or_else(|| {
        use nix::unistd::{Uid, User};
        User::from_uid(Uid::effective())
            .ok()
            .flatten()
            .map(|u| u.dir)
    })
}

#[cfg(windows)]
pub fn home_dir() -> Option<PathBuf> {
    env_path("USERPROFILE").or_else(|| {
        let home_drive = std::env::var_os("HOMEDRIVE")?;
        let home_path = std::env::var_os("HOMEPATH")?;
        let mut path = PathBuf::from(home_drive);
        path.push(home_path);
        Some(path)
    })
}

#[cfg(all(unix, not(target_os = "macos")))]
fn xdg_dir(env_var: &str, fallback: &str) -> Option<PathBuf> {
    env_path(env_var)
        .filter(|p| p.is_absolute())
        .or_else(|| home_dir().map(|h| h.join(fallback)))
}

#[cfg(target_os = "macos")]
pub fn config_dir() -> Option<PathBuf> {
    home_dir().map(|h| h.join("Library").join("Application Support"))
}

#[cfg(windows)]
pub fn config_dir() -> Option<PathBuf> {
    env_path("APPDATA")
}

#[cfg(all(unix, not(target_os = "macos")))]
pub fn config_dir() -> Option<PathBuf> {
    xdg_dir("XDG_CONFIG_HOME", ".config")
}

#[cfg(target_os = "macos")]
pub fn cache_dir() -> Option<PathBuf> {
    home_dir().map(|h| h.join("Library").join("Caches"))
}

#[cfg(windows)]
pub fn cache_dir() -> Option<PathBuf> {
    env_path("LOCALAPPDATA").or_else(|| env_path("APPDATA"))
}

#[cfg(all(unix, not(target_os = "macos")))]
pub fn cache_dir() -> Option<PathBuf> {
    xdg_dir("XDG_CACHE_HOME", ".cache")
}
