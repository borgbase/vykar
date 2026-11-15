use std::io;

use argon2::Block;
use memmap2::MmapMut;

/// Argon2 work buffer mapped directly from the OS.
///
/// The allocator may keep a freed block of this size cached as dirty memory —
/// macOS's system allocator does until memory pressure — which inflates the
/// idle footprint of long-running processes. An anonymous mapping is returned
/// to the OS on drop, so the KDF state also leaves the address space as soon as
/// derivation ends.
pub(crate) struct KdfMemory {
    map: MmapMut,
    block_count: usize,
}

impl KdfMemory {
    /// Map a zero-filled buffer of `block_count` Argon2 blocks.
    pub(crate) fn new(block_count: usize) -> io::Result<Self> {
        let len = block_count
            .checked_mul(Block::SIZE)
            .filter(|&len| len > 0)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid KDF block count {block_count}"),
                )
            })?;
        let map = MmapMut::map_anon(len)?;
        if map.len() != len || map.as_ptr().align_offset(align_of::<Block>()) != 0 {
            return Err(io::Error::other("KDF memory mapping is misaligned"));
        }
        Ok(Self { map, block_count })
    }

    // Alignment is checked in `new`.
    #[allow(unsafe_code, clippy::cast_ptr_alignment)]
    pub(crate) fn as_blocks_mut(&mut self) -> &mut [Block] {
        let ptr = self.map.as_mut_ptr().cast::<Block>();
        // SAFETY: `new` checked that the mapping is exactly
        // `block_count * size_of::<Block>()` bytes (`Block::SIZE` is its size)
        // and that its start is aligned for `Block`. Anonymous mappings are
        // zero-filled, and all-zero bytes are a valid `Block` (`[u64; 128]`).
        // The slice borrows `self` mutably, so the mapping outlives it and no
        // other reference to the bytes can exist while it lives.
        unsafe { std::slice::from_raw_parts_mut(ptr, self.block_count) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_requested_block_count_zeroed() {
        let mut memory = KdfMemory::new(8).unwrap();
        let blocks = memory.as_blocks_mut();
        assert_eq!(blocks.len(), 8);
        assert_eq!(size_of_val(blocks), 8 * Block::SIZE);
        assert!(blocks.iter().all(|b| b.as_ref().iter().all(|&w| w == 0)));
    }

    #[test]
    fn rejects_zero_and_overflowing_block_counts() {
        assert!(KdfMemory::new(0).is_err());
        assert!(KdfMemory::new(usize::MAX).is_err());
    }
}
