use std::io::{self, Read, Take};
use std::sync::LazyLock;

use fastcdc::v2020::{
    AVERAGE_MAX, AVERAGE_MIN, MAXIMUM_MIN, MINIMUM_MAX, MINIMUM_MIN, Normalization, StreamCDC,
    get_gear_with_seed, select_masks,
};
use serde::{Deserialize, Serialize};

/// Hard cap on `ChunkerParams::max_size`. Any user-configured value above
/// this is clamped down during config validation. This bounds the maximum
/// encrypted blob size to `CHUNK_MAX_SIZE_HARD_CAP + 1024` (compression tag +
/// encryption envelope).
pub const CHUNK_MAX_SIZE_HARD_CAP: u32 = 16 * 1024 * 1024; // 16 MiB

pub(crate) fn default_min_size() -> u32 {
    512 * 1024 // 512 KiB
}

pub(crate) fn default_avg_size() -> u32 {
    2 * 1024 * 1024 // 2 MiB
}

pub(crate) fn default_max_size() -> u32 {
    8 * 1024 * 1024 // 8 MiB
}

/// FastCDC parameters as a repository stores them: in its `config` and in
/// every snapshot.
///
/// **On-disk type.** Serialized as a positional 3-element msgpack array in
/// `SnapshotMeta` and in the positional config of formats v2 and v3
/// (`LegacyRepoConfig`), and as a map with named keys inside the v6
/// `config` map. Field order, names, types and the serde attributes are
/// frozen (`legacy_repo_config_golden_bytes`, `v6_config_golden_bytes`,
/// `chunker_params_names_and_serde_defaults_are_pinned`). The YAML
/// `chunker:` section is a separate type, `ChunkerConfig`, that converts
/// into this one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkerParams {
    #[serde(default = "default_min_size")]
    pub min_size: u32,
    #[serde(default = "default_avg_size")]
    pub avg_size: u32,
    #[serde(default = "default_max_size")]
    pub max_size: u32,
}

impl Default for ChunkerParams {
    fn default() -> Self {
        Self {
            min_size: default_min_size(),
            avg_size: default_avg_size(),
            max_size: default_max_size(),
        }
    }
}

impl ChunkerParams {
    /// FastCDC 5 requires even parameters. This check does not normalize or
    /// clamp values, so it is also safe to apply to persisted repository config.
    fn validate_even_sizes(&self) -> vykar_types::error::Result<()> {
        for (field, value) in [
            ("min_size", self.min_size),
            ("avg_size", self.avg_size),
            ("max_size", self.max_size),
        ] {
            if value % 2 != 0 {
                return Err(vykar_types::error::VykarError::Config(format!(
                    "chunker.{field} must be even, got {value}"
                )));
            }
        }
        Ok(())
    }

    /// Everything FastCDC requires of the parameters: the ranges, their
    /// order, and even sizes. Never clamps, so it is safe to apply to the
    /// parameters a repository's `config` holds.
    pub fn validate_stored(&self) -> vykar_types::error::Result<()> {
        use vykar_types::error::VykarError;

        let min = self.min_size as usize;
        let avg = self.avg_size as usize;
        let max = self.max_size as usize;
        let hard_cap = CHUNK_MAX_SIZE_HARD_CAP as usize;

        if !(MINIMUM_MIN..=MINIMUM_MAX).contains(&min) {
            return Err(VykarError::Config(format!(
                "chunker.min_size must be in [{MINIMUM_MIN}, {MINIMUM_MAX}], got {}",
                self.min_size
            )));
        }

        if !(AVERAGE_MIN..=AVERAGE_MAX).contains(&avg) {
            return Err(VykarError::Config(format!(
                "chunker.avg_size must be in [{AVERAGE_MIN}, {AVERAGE_MAX}], got {}",
                self.avg_size
            )));
        }

        if !(MAXIMUM_MIN..=hard_cap).contains(&max) {
            return Err(VykarError::Config(format!(
                "chunker.max_size must be in [{MAXIMUM_MIN}, {CHUNK_MAX_SIZE_HARD_CAP}], got {}",
                self.max_size
            )));
        }

        if self.min_size > self.avg_size {
            return Err(VykarError::Config(format!(
                "chunker must satisfy min_size <= avg_size, got {} > {}",
                self.min_size, self.avg_size
            )));
        }

        if self.avg_size > self.max_size {
            return Err(VykarError::Config(format!(
                "chunker must satisfy avg_size <= max_size, got {} > {}",
                self.avg_size, self.max_size
            )));
        }

        self.validate_even_sizes()
    }
}

/// Chunk a byte slice using FastCDC content-defined chunking.
/// Returns a vector of `(offset, length)` pairs.
#[cfg(test)]
pub(crate) fn chunk_data(data: &[u8], config: &ChunkerParams) -> Vec<(usize, usize)> {
    let chunker = fastcdc::v2020::FastCDC::new(
        data,
        config.min_size as usize,
        config.avg_size as usize,
        config.max_size as usize,
    );
    chunker.map(|chunk| (chunk.offset, chunk.length)).collect()
}

/// Chunk a reader stream using FastCDC content-defined chunking.
pub fn chunk_stream<R: Read>(source: R, config: &ChunkerParams) -> StreamCDC<R> {
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
    config: &ChunkerParams,
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
fn bounded_window_size(limit: u64, config: &ChunkerParams) -> usize {
    let max_size = limit
        .max(u64::from(config.avg_size))
        .max(fastcdc::v2020::MAXIMUM_MIN as u64)
        .min(u64::from(config.max_size)) as usize;
    max_size.next_multiple_of(2)
}

/// fastcdc's default (seed 0) GEAR table and its left-shifted twin.
///
/// fastcdc does not export the tables directly; `get_gear_with_seed(0)`
/// borrows its `'static` arrays. They are copied once into fixed-size arrays
/// so the scan loop indexes them by `u8` without bounds checks, exactly as
/// fastcdc's own `cut` does.
static GEAR_TABLES: LazyLock<([u64; 256], [u64; 256])> = LazyLock::new(|| {
    let (gear, gear_ls) = get_gear_with_seed(0);
    let mut tables = ([0u64; 256], [0u64; 256]);
    for (dst, src) in tables.0.iter_mut().zip(gear.iter()) {
        *dst = *src;
    }
    for (dst, src) in tables.1.iter_mut().zip(gear_ls.iter()) {
        *dst = *src;
    }
    tables
});

/// Resumable form of fastcdc v2020's `cut` loop.
///
/// `cut` hashes the chunk two bytes at a time, starting at pair index
/// `min_size / 2` with hash 0, and tests both bytes of each pair: pairs
/// before `center / 2` against `mask_s`, later pairs against `mask_l`.
/// `center` is `min(avg_size, remaining)`, but every tested pair lies below
/// `remaining / 2`, so the switch point is effectively always `avg_size / 2`.
/// The scan is a forward scan with no lookahead, so testing pairs
/// `[index, end)` of a partial window gives the same verdicts the full
/// window would give for those pairs. That makes the loop resumable from
/// `(index, hash)` once more input has been buffered.
///
/// Returns `Ok(count)` with the same chunk length `cut` returns on a mask
/// hit, or `Err(hash)` when no pair in `[index, end)` hit. `src` holds the
/// chunk bytes from the chunk start and must be at least `2 * end` long.
#[allow(clippy::indexing_slicing, clippy::too_many_arguments)]
#[inline]
fn scan_pairs(
    src: &[u8],
    mut index: usize,
    end: usize,
    mut hash: u64,
    split: usize,
    mask_s: u64,
    mask_l: u64,
    gear: &[u64; 256],
    gear_ls: &[u64; 256],
) -> Result<usize, u64> {
    let src = &src[..2 * end];
    let (mask_s_ls, mask_l_ls) = (mask_s << 1, mask_l << 1);
    let limit1 = split.min(end);
    while index < limit1 {
        let a = index * 2;
        hash = (hash << 2).wrapping_add(gear_ls[src[a] as usize]);
        if (hash & mask_s_ls) == 0 {
            return Ok(a);
        }
        hash = hash.wrapping_add(gear[src[a + 1] as usize]);
        if (hash & mask_s) == 0 {
            return Ok(a + 1);
        }
        index += 1;
    }
    while index < end {
        let a = index * 2;
        hash = (hash << 2).wrapping_add(gear_ls[src[a] as usize]);
        if (hash & mask_l_ls) == 0 {
            return Ok(a);
        }
        hash = hash.wrapping_add(gear[src[a + 1] as usize]);
        if (hash & mask_l) == 0 {
            return Ok(a + 1);
        }
        index += 1;
    }
    Err(hash)
}

/// Bounded FastCDC reader that lends chunks from its own read buffer.
///
/// The buffer has the same size and lifetime as the old bounded StreamCDC
/// buffer: one allocation per file or segment, released when the read ends.
/// It never grows and is not retained by an idle worker. Chunks are only
/// copied when the caller needs to retain their raw bytes.
///
/// Cut points are identical to fastcdc v2020's `cut` over a full window of
/// `buf.len()` bytes (or the rest of the input). Instead of compacting and
/// refilling the buffer before every cut, the reader scans what is already
/// buffered and only compacts + refills when that is not enough to decide,
/// resuming the scan where it stopped (see [`scan_pairs`]).
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
    pub(crate) fn new(source: Take<R>, config: &ChunkerParams) -> Self {
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

    /// Compact the unconsumed bytes to the front and read until the buffer
    /// is full or the bounded source is exhausted.
    /// `pos <= len <= buf.len()` throughout; only called between chunks, so
    /// no lent chunk is alive.
    #[allow(clippy::indexing_slicing)]
    fn refill(&mut self) -> io::Result<()> {
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

    /// The next chunk borrows the buffer until the next call.
    ///
    /// The window `cut` would see is `buf[pos..len]` once it is *complete*:
    /// either it spans the whole buffer (the maximum chunk size) or the
    /// source is exhausted. Until then only pairs that lie entirely inside
    /// the buffered bytes are tested; a hit there is final. Otherwise the
    /// buffer is refilled and the scan resumes at the next untested pair.
    #[allow(clippy::indexing_slicing)]
    pub(crate) fn next_chunk(&mut self) -> io::Result<Option<&[u8]>> {
        let (gear, gear_ls) = &*GEAR_TABLES;
        let split = self.avg_size / 2;
        let mut index = self.min_size / 2;
        let mut hash = 0u64;
        loop {
            let avail = self.len - self.pos;
            let complete = self.eof || avail == self.buf.len();
            if complete && avail <= self.min_size {
                // `cut` returns short windows whole; an empty one ends the read.
                if avail == 0 {
                    return Ok(None);
                }
                return Ok(Some(self.take(avail)));
            }
            let end = avail / 2;
            if index < end {
                match scan_pairs(
                    &self.buf[self.pos..self.len],
                    index,
                    end,
                    hash,
                    split,
                    self.mask_s,
                    self.mask_l,
                    gear,
                    gear_ls,
                ) {
                    Ok(count) => return Ok(Some(self.take(count))),
                    Err(h) => {
                        hash = h;
                        index = end;
                    }
                }
            }
            if complete {
                // No hit in the whole window: `cut` returns all of it,
                // including an odd trailing byte that was never tested.
                return Ok(Some(self.take(avail)));
            }
            // Not decided yet. `refill` makes progress: either `pos > 0`
            // and compaction frees space, or `pos == 0` and the window is
            // short of `buf.len()`; a read of 0 sets `eof`.
            self.refill()?;
        }
    }

    /// Lend `count` bytes from `pos`; the caller guarantees
    /// `pos + count <= len`.
    #[allow(clippy::indexing_slicing)]
    fn take(&mut self, count: usize) -> &[u8] {
        let start = self.pos;
        self.pos += count;
        &self.buf[start..self.pos]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn reader_buffer_matches_legacy_bound_and_never_grows() {
        let config = ChunkerParams {
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

    fn xorshift_data(seed: u64, len: usize) -> Vec<u8> {
        let mut s = seed | 1;
        (0..len)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 24) as u8
            })
            .collect()
    }

    /// The pre-change reader: compact and refill a full window before every
    /// chunk, then call fastcdc's `cut` on it. This is the reference the
    /// resumable scan must match bit for bit.
    fn reference_chunks(data: &[u8], limit: usize, config: &ChunkerParams) -> Vec<usize> {
        let data = &data[..limit.min(data.len())];
        let window = bounded_window_size(limit as u64, config);
        let (mask_s, mask_l) = select_masks(config.avg_size as usize, Normalization::Level1);
        let mut out = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            let end = (pos + window).min(data.len());
            let (_, count) = fastcdc::v2020::cut(
                &data[pos..end],
                config.min_size as usize,
                config.avg_size as usize,
                window,
                mask_s,
                mask_l,
                mask_s << 1,
                mask_l << 1,
            );
            assert!(count > 0);
            out.push(count);
            pos += count;
        }
        out
    }

    /// Returns reads of pseudo-random lengths (1..=max_read) and an
    /// occasional `Interrupted`, to exercise every refill boundary.
    struct ChoppyReader<'a> {
        data: &'a [u8],
        pos: usize,
        state: u64,
        max_read: usize,
    }

    impl Read for ChoppyReader<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.state ^= self.state << 13;
            self.state ^= self.state >> 7;
            self.state ^= self.state << 17;
            if self.state.is_multiple_of(7) {
                return Err(io::ErrorKind::Interrupted.into());
            }
            let want = 1 + (self.state as usize >> 8) % self.max_read;
            let n = want.min(buf.len()).min(self.data.len() - self.pos);
            buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    fn reader_chunks<R: Read>(source: Take<R>, data: &[u8], config: &ChunkerParams) -> Vec<usize> {
        let mut reader = ChunkReader::new(source, config);
        let mut out = Vec::new();
        let mut offset = 0;
        while let Some(chunk) = reader.next_chunk().unwrap() {
            assert_eq!(chunk, &data[offset..offset + chunk.len()]);
            out.push(chunk.len());
            offset += chunk.len();
        }
        assert!(reader.next_chunk().unwrap().is_none());
        out
    }

    fn assert_matches_reference(data: &[u8], limit: usize, config: &ChunkerParams, seed: u64) {
        let expected = reference_chunks(data, limit, config);
        let whole = reader_chunks(Cursor::new(data).take(limit as u64), data, config);
        assert_eq!(
            whole,
            expected,
            "full reads, len {} limit {limit}",
            data.len()
        );
        for max_read in [1, 3, 777, config.max_size as usize / 3 + 1, usize::MAX / 2] {
            let choppy = ChoppyReader {
                data,
                pos: 0,
                state: seed | 1,
                max_read,
            };
            let got = reader_chunks(choppy.take(limit as u64), data, config);
            assert_eq!(
                got,
                expected,
                "choppy reads ({max_read}), len {} limit {limit}",
                data.len()
            );
        }
    }

    fn differential_configs() -> Vec<ChunkerParams> {
        vec![
            ChunkerParams {
                min_size: 256,
                avg_size: 1024,
                max_size: 4096,
            },
            // Low-entropy masks so `mask_l` hits past `avg` are common.
            ChunkerParams {
                min_size: 64,
                avg_size: 256,
                max_size: 1024,
            },
            // min close to avg: the mask_s phase is short.
            ChunkerParams {
                min_size: 1000,
                avg_size: 1024,
                max_size: 1100,
            },
        ]
    }

    #[test]
    fn resumable_scan_matches_fastcdc_cut_around_size_limits() {
        for config in differential_configs() {
            let (min, avg, max) = (
                config.min_size as usize,
                config.avg_size as usize,
                config.max_size as usize,
            );
            let mut lens = vec![0, 1, 2, 3, 63, 64, 65];
            for base in [min, avg, max, 2 * max, 3 * max + avg] {
                lens.extend([base - 1, base, base + 1, base + 2]);
            }
            for (i, &len) in lens.iter().enumerate() {
                let seed = 0x9e37_79b9 + i as u64;
                let random = xorshift_data(seed, len);
                let zeros = vec![0u8; len];
                for data in [&random, &zeros] {
                    for limit in [
                        len,
                        len + 1,
                        len.saturating_sub(1),
                        len / 2 + 1,
                        usize::MAX / 4,
                    ] {
                        assert_matches_reference(data, limit, &config, seed);
                    }
                }
            }
        }
    }

    #[test]
    fn resumable_scan_matches_fastcdc_cut_on_long_mixed_inputs() {
        for (n, config) in differential_configs().into_iter().enumerate() {
            let max = config.max_size as usize;
            for odd in [0, 1] {
                let len = 40 * max + odd + n;
                let mut data = xorshift_data(42 + n as u64, len);
                // Zero runs force max-size cuts and resume across refills.
                for run in data.chunks_mut(7 * max + 13).skip(1).step_by(2) {
                    let k = run.len().min(2 * max + 5);
                    run[..k].fill(0);
                }
                for limit in [len, len / 3 + 1, 5 * max - 1] {
                    assert_matches_reference(&data, limit, &config, 7 + odd as u64);
                }
            }
        }
    }

    #[test]
    fn resumable_scan_matches_fastcdc_cut_at_default_parameters() {
        let config = ChunkerParams::default();
        let len = 3 * config.max_size as usize + 12_345;
        let data = xorshift_data(2026, len);
        assert_matches_reference(&data, len, &config, 11);
        assert_matches_reference(&data, len - 4_000_001, &config, 13);
    }

    /// Stored parameters are checked, never clamped.
    #[test]
    fn params_validate_stored_rejects_without_clamping() {
        let config = ChunkerParams {
            min_size: 512 * 1024,
            avg_size: 2 * 1024 * 1024,
            max_size: 2 * CHUNK_MAX_SIZE_HARD_CAP,
        };
        let error = config.validate_stored().unwrap_err();
        assert!(error.to_string().contains("chunker.max_size must be in"));
        for (min, avg, max) in [(0, 1024, 4096), (256, 0, 4096), (8192, 4096, 16384)] {
            let config = ChunkerParams {
                min_size: min,
                avg_size: avg,
                max_size: max,
            };
            assert!(config.validate_stored().is_err(), "{min} {avg} {max}");
        }
        assert!(ChunkerParams::default().validate_stored().is_ok());
    }
}
