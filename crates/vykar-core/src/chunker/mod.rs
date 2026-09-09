use std::io::{Read, Take};

use fastcdc::v2020::{ChunkData, Error, StreamCDC};

use crate::config::ChunkerConfig;

// Size parameters are persisted at init. FastCDC 5 requires even parameters,
// but existing repositories may contain odd ones. Use the original algorithm
// for those repositories instead of rounding and changing chunk identity.
fn needs_legacy(config: &ChunkerConfig) -> bool {
    [config.min_size, config.avg_size, config.max_size]
        .iter()
        .any(|size| size % 2 != 0)
}

enum ChunkStream<R: Read> {
    Current(StreamCDC<R>),
    Legacy(fastcdc_legacy::v2020::StreamCDC<R>),
}

impl<R: Read> Iterator for ChunkStream<R> {
    type Item = Result<ChunkData, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Current(stream) => stream.next(),
            Self::Legacy(stream) => stream.next().map(|result| {
                result
                    .map(|chunk| ChunkData {
                        hash: chunk.hash,
                        offset: chunk.offset,
                        length: chunk.length,
                        data: chunk.data,
                    })
                    .map_err(|error| match error {
                        fastcdc_legacy::v2020::Error::Empty => Error::Empty,
                        fastcdc_legacy::v2020::Error::IoError(error) => Error::IoError(error),
                        fastcdc_legacy::v2020::Error::Other(message) => Error::Other(message),
                    })
            }),
        }
    }
}

/// Chunk a byte slice using FastCDC content-defined chunking.
/// Returns a vector of `(offset, length)` pairs.
#[cfg(test)]
pub(crate) fn chunk_data(data: &[u8], config: &ChunkerConfig) -> Vec<(usize, usize)> {
    if needs_legacy(config) {
        return fastcdc_legacy::v2020::FastCDC::new(
            data,
            config.min_size as usize,
            config.avg_size as usize,
            config.max_size as usize,
        )
        .map(|chunk| (chunk.offset, chunk.length))
        .collect();
    }
    let chunker = fastcdc::v2020::FastCDC::new(
        data,
        config.min_size as usize,
        config.avg_size as usize,
        config.max_size as usize,
    );
    chunker.map(|chunk| (chunk.offset, chunk.length)).collect()
}

/// Chunk a reader stream using FastCDC content-defined chunking.
pub fn chunk_stream<R: Read>(
    source: R,
    config: &ChunkerConfig,
) -> impl Iterator<Item = Result<ChunkData, Error>> {
    stream_with_max(source, config, config.max_size as usize)
}

fn stream_with_max<R: Read>(source: R, config: &ChunkerConfig, max_size: usize) -> ChunkStream<R> {
    if needs_legacy(config) {
        return ChunkStream::Legacy(fastcdc_legacy::v2020::StreamCDC::new(
            source,
            config.min_size as usize,
            config.avg_size as usize,
            max_size,
        ));
    }
    ChunkStream::Current(StreamCDC::new(
        source,
        config.min_size as usize,
        config.avg_size as usize,
        max_size,
    ))
}

/// Chunk a bounded reader without reserving more than its input can use.
///
/// StreamCDC allocates `max_size` bytes per reader. Backup already limits
/// reads to a file or segment, so smaller inputs need no full-sized buffer.
/// Keep the average-size floor to preserve FastCDC's parameter ordering.
/// The cut points are unchanged: the reduced maximum is still at least the
/// reader's limit (unless the original maximum was smaller), and the minimum
/// and average, which determine the cut masks, stay the same.
///
/// For even repository parameters, round the derived maximum *up* to even
/// for FastCDC 5. It cannot exceed the even repository cap. Odd repository
/// parameters use FastCDC 4 with the original, unrounded maximum, including
/// when the derived maximum happens to be even.
pub(crate) fn chunk_stream_bounded<R: Read>(
    source: Take<R>,
    config: &ChunkerConfig,
) -> impl Iterator<Item = Result<ChunkData, Error>> {
    let max_size = source
        .limit()
        .max(u64::from(config.avg_size))
        .max(fastcdc::v2020::MAXIMUM_MIN as u64)
        .min(u64::from(config.max_size)) as usize;
    let max_size = if needs_legacy(config) {
        max_size
    } else {
        max_size.next_multiple_of(2)
    };
    stream_with_max(source, config, max_size)
}
