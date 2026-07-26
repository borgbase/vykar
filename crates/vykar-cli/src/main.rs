use std::process::ExitCode;

// musl's built-in allocator has a single global lock, which serialises the
// chunking and upload threads. mimalloc removes that bottleneck on the static
// Linux builds; every other target keeps the system allocator.
#[cfg(target_env = "musl")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    vykar_cli::run()
}
