// File-level unsafe_code allow: this file makes one libc syscall
// (sync_file_range). The `unsafe { }` block has a `// SAFETY:` comment
// enforced by clippy::undocumented_unsafe_blocks.
#![allow(unsafe_code)]

//! Early writeback for uploads that end in an `fdatasync`.
//!
//! A pack PUT is acknowledged only after `fdatasync`, which on a 30-70 MiB
//! upload left almost all of the file dirty in the page cache until the last
//! body byte arrived, so the client waited for the whole flush (median 63 ms,
//! p90 260 ms at 4 concurrent uploads). Starting writeback while the body is
//! still streaming moves that work under the network transfer.
//!
//! This is purely a hint: it does not wait for the I/O, does not flush
//! metadata, and its errors are ignored. Durability still comes from the
//! final `fdatasync` + rename + directory fsync, and a write error raised by
//! the early writeback is still reported by that `fdatasync` (only a waiting
//! sync consumes the file's writeback error state).

use std::fs::File;

/// Bytes of new body data between two writeback kicks. Large enough that the
/// extra syscalls are negligible, small enough that the final `fdatasync`
/// finds at most a few MiB still dirty.
pub(crate) const WRITEBACK_INTERVAL: u64 = 8 * 1024 * 1024;

/// Whether [`start_writeback`] does anything. Elsewhere the caller skips the
/// kicks entirely, so no extra handle to the temp file is ever held (Windows
/// cannot rename or unlink a file with an open handle).
pub(crate) const SUPPORTED: bool = cfg!(target_os = "linux");

/// Ask the kernel to start writing back every dirty page of `file`.
#[cfg(target_os = "linux")]
pub(crate) fn start_writeback(file: &File) {
    use std::os::fd::AsRawFd;
    // SAFETY: `file` is a live, owned descriptor for the duration of the call;
    // sync_file_range takes no pointers. offset 0 + nbytes 0 means "to EOF".
    let _ = unsafe { libc::sync_file_range(file.as_raw_fd(), 0, 0, libc::SYNC_FILE_RANGE_WRITE) };
}

/// No portable equivalent; the final sync does all the work.
#[cfg(not(target_os = "linux"))]
pub(crate) fn start_writeback(_file: &File) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// The hint must be harmless on a regular file, including an empty one.
    #[test]
    fn start_writeback_on_temp_file() {
        let mut f = tempfile::tempfile().unwrap();
        start_writeback(&f);
        f.write_all(&vec![7u8; 64 * 1024]).unwrap();
        start_writeback(&f);
        vykar_common::fs::fdatasync_file(&f).unwrap();
    }
}
