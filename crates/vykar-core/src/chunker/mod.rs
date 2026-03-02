use std::io::Read;

use fastcdc::v2020::{FastCDC, StreamCDC};

use crate::config::ChunkerConfig;

/// Chunk a byte slice using FastCDC content-defined chunking.
/// Returns a vector of `(offset, length)` pairs.
pub fn chunk_data(data: &[u8], config: &ChunkerConfig) -> Vec<(usize, usize)> {
    let chunker = FastCDC::new(data, config.min_size, config.avg_size, config.max_size);
    chunker.map(|chunk| (chunk.offset, chunk.length)).collect()
}

/// Chunk a reader stream using FastCDC content-defined chunking.
pub fn chunk_stream<R: Read>(source: R, config: &ChunkerConfig) -> StreamCDC<R> {
    StreamCDC::new(source, config.min_size, config.avg_size, config.max_size)
}
