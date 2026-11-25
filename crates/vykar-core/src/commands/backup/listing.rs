//! The one writer of snapshot listings (backup, command dumps, the
//! `check --repair` rewriter): entries are encoded into the item stream
//! one at a time, the stream is cut into tree chunks after whole entries
//! (see [`ItemStreamCutter`]), and each chunk is stored as it closes,
//! through [`WriteSession::store`].
//!
//! Entries are encoded in snapshot format `CURRENT_FORMAT_VERSION`, which
//! [`ListingWriter::finish`] reports.
//!
//! Invariant: a tree chunk holds whole entries, so it decodes on its own.
//! The mark and `check` decode each distinct tree chunk once and rely
//! on this; they reject a chunk that ends inside an entry.
//!
//! The writer also builds the listing's key index (06 step 4, see
//! [`crate::snapshot::listing`]) from the paths it appends.
//! [`ListingWriter::begin_segment`] starts a listing segment (a source root,
//! or the command dumps) at a tree-chunk boundary, closing the open chunk
//! even when it is below the minimum size. The first entry that breaks order
//! 1 (out of order, outside its segment root, or with an empty path
//! component) makes the index order 0, which readers scan.

use vykar_types::chunk_id::{ChunkHasher, ChunkId};
use vykar_types::error::Result;

use super::item_cut::ItemStreamCutter;
use crate::compress::Compression;
use crate::repo::pack::PackType;
use crate::repo::{ChunkInput, Repo, WriteSession};
use crate::snapshot::item::Item;
use crate::snapshot::listing::{ListingIndexBuilder, encode_ext};

/// Accumulates one snapshot's listing. No `Debug`: the cutter's key
/// derives from the chunk-ID key.
pub(crate) struct ListingWriter {
    cutter: ItemStreamCutter,
    compression: Compression,
    /// Encoded entries of the open tree chunk.
    buf: Vec<u8>,
    item_ptrs: Vec<ChunkId>,
    index: ListingIndexBuilder,
}

/// What a finished listing contributes to `SnapshotMeta`.
#[derive(Debug)]
pub(crate) struct FinishedListing {
    pub(crate) item_ptrs: Vec<ChunkId>,
    /// The snapshot format the tree chunks are encoded in.
    pub(crate) format_version: u32,
    /// `SnapshotMeta.ext`: the encoded key index (06 step 4). Always `Some`.
    pub(crate) ext: Option<Vec<u8>>,
}

impl ListingWriter {
    pub(crate) fn new(chunk_hasher: &ChunkHasher, compression: Compression) -> Self {
        Self {
            cutter: ItemStreamCutter::new(chunk_hasher),
            compression,
            buf: Vec::new(),
            item_ptrs: Vec::new(),
            index: ListingIndexBuilder::new(),
        }
    }

    /// [`Self::new`] with an explicit cutter, so tests can force many small
    /// tree chunks.
    #[cfg(test)]
    pub(crate) fn with_cutter(cutter: ItemStreamCutter, compression: Compression) -> Self {
        Self {
            cutter,
            compression,
            buf: Vec::new(),
            item_ptrs: Vec::new(),
            index: ListingIndexBuilder::new(),
        }
    }

    /// Start a listing segment (one per resolved source root, and one for
    /// the command dumps): closes the open tree chunk so the segment begins
    /// at a chunk boundary, and records `root` for the key index.
    pub(crate) fn begin_segment(
        &mut self,
        repo: &Repo,
        ws: &mut WriteSession,
        root: Vec<u8>,
    ) -> Result<()> {
        self.close_chunk(repo, ws)?;
        self.index.begin_segment(root);
        Ok(())
    }

    /// Append `item` to the listing. Closes and stores the open tree
    /// chunk right after it when the cut rule says so.
    pub(crate) fn push(&mut self, repo: &Repo, ws: &mut WriteSession, item: &Item) -> Result<()> {
        let start = self.buf.len();
        self.index
            .observe(item.path_bytes(), self.item_ptrs.len(), start == 0);
        rmp_serde::encode::write(&mut self.buf, item)?;
        self.end_entry(repo, ws, &item.path, start)
    }

    /// `push` for an entry already encoded, for tests that write
    /// format-1 listings. `entry` must be exactly one encoded `Item`.
    #[cfg(test)]
    pub(crate) fn push_encoded(
        &mut self,
        repo: &Repo,
        ws: &mut WriteSession,
        path: &str,
        entry: &[u8],
    ) -> Result<()> {
        let start = self.buf.len();
        self.index
            .observe(path.as_bytes(), self.item_ptrs.len(), start == 0);
        self.buf.extend_from_slice(entry);
        self.end_entry(repo, ws, path, start)
    }

    /// Cut after the entry that starts at `start` when the rule says so.
    fn end_entry(
        &mut self,
        repo: &Repo,
        ws: &mut WriteSession,
        path: &str,
        start: usize,
    ) -> Result<()> {
        let entry_len = self.buf.len() - start;
        if self.cutter.should_cut(path, entry_len, self.buf.len()) {
            self.close_chunk(repo, ws)?;
        }
        Ok(())
    }

    /// Store the last, partial tree chunk and return the listing.
    pub(crate) fn finish(mut self, repo: &Repo, ws: &mut WriteSession) -> Result<FinishedListing> {
        self.close_chunk(repo, ws)?;
        let index = std::mem::replace(&mut self.index, ListingIndexBuilder::new())
            .finish(self.item_ptrs.len());
        Ok(FinishedListing {
            item_ptrs: std::mem::take(&mut self.item_ptrs),
            format_version: crate::snapshot::CURRENT_FORMAT_VERSION,
            ext: Some(encode_ext(&index)?),
        })
    }

    fn close_chunk(&mut self, repo: &Repo, ws: &mut WriteSession) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::take(&mut self.buf);
        let stored = ws.store(
            repo,
            ChunkInput::Plain {
                data: &chunk,
                compression: self.compression,
            },
            PackType::Tree,
        )?;
        self.item_ptrs.push(stored.chunk_ref.id);
        Ok(())
    }
}
