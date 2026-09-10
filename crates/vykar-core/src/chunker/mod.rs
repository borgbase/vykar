use std::io::{self, Read, Take};

use fastcdc::v2020::{cut, select_masks, Normalization, StreamCDC};

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
///
/// The derived maximum is rounded *up* to an even value. fastcdc 5 requires
/// even size parameters, and this one is derived from a reader limit — a file
/// or segment length — which is odd about half the time. The assertions are
/// `debug_assert!`, so an odd value would not panic in a release build; it
/// would chunk with unintended masks instead. Rounding up rather than down is
/// what keeps the cut points identical: rounding a limit of 1025 down to 1024
/// would force a cut the unbounded chunker does not make. It cannot exceed
/// `config.max_size` either: init and backup validate that the repository's
/// parameters are even, so an odd derived value is below that even cap.
#[cfg(test)]
pub(crate) fn chunk_stream_bounded<R: Read>(
    source: Take<R>,
    config: &ChunkerConfig,
) -> StreamCDC<Take<R>> {
    let max_size = bounded_window_size(source.limit(), config);
    StreamCDC::new(
        source,
        config.min_size as usize,
        config.avg_size as usize,
        max_size,
    )
}

/// Preserve the original bounded StreamCDC allocation size and cut window.
fn bounded_window_size(limit: u64, config: &ChunkerConfig) -> usize {
    let max_size = limit
        .max(u64::from(config.avg_size))
        .max(fastcdc::v2020::MAXIMUM_MIN as u64)
        .min(u64::from(config.max_size)) as usize;
    max_size.next_multiple_of(2)
}

/// Bounded FastCDC reader that lends chunks from its own read buffer.
///
/// The buffer has the same size and lifetime as the old bounded StreamCDC
/// buffer: one allocation per file or segment, released when the read ends.
/// It never grows and is not retained by an idle worker. Chunks are only
/// copied when the caller needs to retain their raw bytes.
pub(crate) struct ChunkReader<R: Read> {
    source: Take<R>,
    buf: Vec<u8>,
    pos: usize,
    len: usize,
    eof: bool,
    min_size: usize,
    avg_size: usize,
    mask_s: u64,
    mask_l: u64,
}

impl<R: Read> ChunkReader<R> {
    pub(crate) fn new(source: Take<R>, config: &ChunkerConfig) -> Self {
        let max_size = bounded_window_size(source.limit(), config);
        let (mask_s, mask_l) = select_masks(config.avg_size as usize, Normalization::Level1);
        Self {
            source,
            buf: vec![0; max_size],
            pos: 0,
            len: 0,
            eof: false,
            min_size: config.min_size as usize,
            avg_size: config.avg_size as usize,
            mask_s,
            mask_l,
        }
    }

    /// Fill a complete cut window unless the bounded source is exhausted.
    /// `pos <= len <= buf.len()` throughout; consumed bytes are compacted
    /// before refilling, after the caller has finished borrowing the chunk.
    #[allow(clippy::indexing_slicing)]
    fn fill(&mut self) -> io::Result<()> {
        if self.eof {
            return Ok(());
        }
        if self.pos > 0 {
            self.buf.copy_within(self.pos..self.len, 0);
            self.len -= self.pos;
            self.pos = 0;
        }
        while self.len < self.buf.len() {
            match self.source.read(&mut self.buf[self.len..]) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(n) => self.len += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// The next chunk borrows the buffer until the next call. `cut` returns
    /// a length within the live window, so `pos + count <= len`.
    #[allow(clippy::indexing_slicing)]
    pub(crate) fn next_chunk(&mut self) -> io::Result<Option<&[u8]>> {
        self.fill()?;
        if self.pos == self.len {
            return Ok(None);
        }
        let (_hash, count) = cut(
            &self.buf[self.pos..self.len],
            self.min_size,
            self.avg_size,
            self.buf.len(),
            self.mask_s,
            self.mask_l,
            self.mask_s << 1,
            self.mask_l << 1,
        );
        if count == 0 {
            return Ok(None);
        }
        let start = self.pos;
        self.pos += count;
        Ok(Some(&self.buf[start..self.pos]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn reader_buffer_matches_legacy_bound_and_never_grows() {
        let config = ChunkerConfig {
            min_size: 256,
            avg_size: 1024,
            max_size: 4096,
        };
        for (limit, capacity) in [
            (0, 1024),
            (1, 1024),
            (1025, 1026),
            (3001, 3002),
            (9000, 4096),
        ] {
            let data = vec![0x42; limit];
            let mut reader = ChunkReader::new(Cursor::new(&data).take(limit as u64), &config);
            assert_eq!(reader.buf.capacity(), capacity);
            let mut total = 0;
            while let Some(chunk) = reader.next_chunk().unwrap() {
                total += chunk.len();
                assert_eq!(reader.buf.capacity(), capacity);
            }
            assert_eq!(total, limit);
            assert_eq!(reader.buf.capacity(), capacity);
        }
    }
}
