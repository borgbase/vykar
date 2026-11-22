//! The one writer of snapshot listings (backup, command dumps, the
//! `check --repair` rewriter): entries are encoded into the item stream
//! one at a time, the stream is cut into tree chunks after whole entries
//! (see [`ItemStreamCutter`]), and each chunk is stored as it closes.
//!
//! Invariant: a tree chunk holds whole entries, so it decodes on its own.
//! The mark and `check` decode each distinct tree chunk once and rely
//! on this; they reject a chunk that ends inside an entry.

use vykar_types::chunk_id::{ChunkHasher, ChunkId};
use vykar_types::error::Result;

use super::item_cut::ItemStreamCutter;
use crate::compress::Compression;
use crate::repo::Repository;
use crate::repo::pack::PackType;
use crate::snapshot::item::Item;

/// Accumulates one snapshot's listing. No `Debug`: the cutter's key
/// derives from the chunk-ID key.
pub(crate) struct ListingWriter {
    cutter: ItemStreamCutter,
    compression: Compression,
    /// Encoded entries of the open tree chunk.
    buf: Vec<u8>,
    item_ptrs: Vec<ChunkId>,
}

/// What a finished listing contributes to `SnapshotMeta`.
#[derive(Debug)]
pub(crate) struct FinishedListing {
    pub(crate) item_ptrs: Vec<ChunkId>,
}

impl ListingWriter {
    pub(crate) fn new(chunk_hasher: &ChunkHasher, compression: Compression) -> Self {
        Self {
            cutter: ItemStreamCutter::new(chunk_hasher),
            compression,
            buf: Vec::new(),
            item_ptrs: Vec::new(),
        }
    }

    /// Append `item` to the listing. Closes and stores the open tree
    /// chunk right after it when the cut rule says so.
    pub(crate) fn push(&mut self, repo: &mut Repository, item: &Item) -> Result<()> {
        let start = self.buf.len();
        rmp_serde::encode::write(&mut self.buf, item)?;
        let entry_len = self.buf.len() - start;
        if self
            .cutter
            .should_cut(&item.path, entry_len, self.buf.len())
        {
            self.close_chunk(repo)?;
        }
        Ok(())
    }

    /// Store the last, partial tree chunk and return the listing.
    pub(crate) fn finish(mut self, repo: &mut Repository) -> Result<FinishedListing> {
        self.close_chunk(repo)?;
        Ok(FinishedListing {
            item_ptrs: std::mem::take(&mut self.item_ptrs),
        })
    }

    fn close_chunk(&mut self, repo: &mut Repository) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::take(&mut self.buf);
        let (chunk_id, _csize, _is_new) =
            repo.store_chunk(&chunk, self.compression, PackType::Tree)?;
        self.item_ptrs.push(chunk_id);
        Ok(())
    }
}
