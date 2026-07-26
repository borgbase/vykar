#![allow(clippy::print_stderr)]

use std::process::ExitCode;

use clap::Parser;

use vykar_server::Cli;

// musl's built-in allocator has a single global lock, which serialises the
// request handler threads. mimalloc removes that bottleneck on the static
// Linux builds that back the published images; every other target keeps the
// system allocator.
#[cfg(target_env = "musl")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    let cli = Cli::parse();
    match vykar_server::run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e}");
            ExitCode::FAILURE
        }
    }
}
