use std::io::{Read, Take};

use fastcdc::v2020::StreamCDC;

use crate::config::ChunkerConfig;

/// Chunk a byte slice using FastCDC content-defined chunking.
/// Returns a vector of `(offset, length)` pairs.
#[cfg(test)]
pub(crate) fn chunk_data(data: &[u8], config: &ChunkerConfig) -> Vec<(usize, usize)> {
    let chunker = fastcdc::v2020::FastCDC::new(
        data,
        config.min_size as usize,
        config.avg_size as usize,
        config.max_size as usize,
    );
    chunker.map(|chunk| (chunk.offset, chunk.length)).collect()
}

/// Chunk a reader stream using FastCDC content-defined chunking.
pub fn chunk_stream<R: Read>(source: R, config: &ChunkerConfig) -> StreamCDC<R> {
    StreamCDC::new(
        source,
        config.min_size as usize,
        config.avg_size as usize,
        config.max_size as usize,
    )
}

/// Chunk a bounded reader without reserving more than its input can use.
///
/// StreamCDC allocates `max_size` bytes per reader. Backup already limits
/// reads to a file or segment, so smaller inputs need no full-sized buffer.
/// Keep the average-size floor to preserve FastCDC's parameter ordering.
/// The cut points are unchanged: the reduced maximum is still at least the
/// reader's limit (unless the original maximum was smaller), and the minimum
/// and average, which determine the cut masks, stay the same.
pub(crate) fn chunk_stream_bounded<R: Read>(
    source: Take<R>,
    config: &ChunkerConfig,
) -> StreamCDC<Take<R>> {
    let max_size = source
        .limit()
        .max(u64::from(config.avg_size))
        .max(fastcdc::v2020::MAXIMUM_MIN as u64)
        .min(u64::from(config.max_size)) as usize;
    StreamCDC::new(
        source,
        config.min_size as usize,
        config.avg_size as usize,
        max_size,
    )
}
