use std::sync::LazyLock;

/// Tokio runtime used by async-backed storage adapters (native SFTP) to bridge
/// into synchronous call sites. Created lazily on first use.
pub(crate) static ASYNC_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    let worker_threads = std::thread::available_parallelism()
        .map(|n| n.get().clamp(4, 8))
        .unwrap_or(4);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .expect("failed to create tokio runtime for blocking layer")
});
