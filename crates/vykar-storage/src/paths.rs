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
