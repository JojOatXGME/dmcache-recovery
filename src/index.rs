use std::{io, iter};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::mem::size_of;
use std::path::Path;

use memmap2::{Mmap, MmapMut, MmapOptions};

const BLOCK_SIZE_OFFSET: usize = 48;
const CAPACITY_OFFSET: usize = 56;
const BITSET_OFFSET: usize = 64;
const PREAMBLE_SIZE: usize = BLOCK_SIZE_OFFSET;
const PREAMBLE: &[u8; PREAMBLE_SIZE] = b"INDEX / dmcache-recovery\n\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
const U64_SIZE: usize = size_of::<u64>();

pub(crate) struct Index {
    mmap: Mmap,
    layout: Layout,
    block_size: u64,
}

impl Index {
    pub(crate) fn open(path: &Path) -> io::Result<Index> {
        let file = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        #[cfg(unix)]
        mmap.advise(memmap2::Advice::Random)?;
        let layout = Layout::from_file(&mmap)?;
        let block_size = Layout::get_block_size(&mmap);
        Ok(Index { mmap, layout, block_size })
    }

    pub(crate) fn get_block_size(&self) -> usize {
        self.block_size as usize
    }

    pub(crate) fn get<'a>(&'a self, hash: &'a [u8]) -> impl Iterator<Item=usize> + 'a {
        read_hash_indices(&self.layout, hash)
            .take_while(|index| self.layout.is_used(&self.mmap, *index))
            .filter(|index| self.hash_matches(*index, hash))
            .map(|index| self.layout.get_value(&self.mmap, index) as usize)
    }

    fn hash_matches(&self, index: usize, mut hash: &[u8]) -> bool {
        let mut buf: [u8; U64_SIZE] = [0; U64_SIZE];
        write_bytes(&mut hash, &mut buf);
        if u64::from_ne_bytes(buf) != self.layout.get_hash1(&self.mmap, index) {
            return false;
        }
        write_bytes(&mut hash, &mut buf);
        if u64::from_ne_bytes(buf) != self.layout.get_hash2(&self.mmap, index) {
            return false;
        }
        write_bytes(&mut hash, &mut buf);
        if u64::from_ne_bytes(buf) != self.layout.get_hash3(&self.mmap, index) {
            return false;
        }
        true
    }
}

pub(crate) struct IndexBuilder {
    mmap: MmapMut,
    layout: Layout,
    closed: bool,
}

impl IndexBuilder {
    pub(crate) fn new(path: &Path, item_count: usize, block_size: usize) -> io::Result<IndexBuilder> {
        let layout = Layout::from_item_count(item_count);
        let file = OpenOptions::new().read(true).write(true).create_new(true).open(path)?;
        file.set_len(layout.min_file_size as u64)?;
        let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        #[cfg(unix)]
        mmap.advise(memmap2::Advice::Random)?;
        Layout::set_block_size(&mut mmap, block_size);
        layout.set_capacity(&mut mmap);
        Ok(IndexBuilder { mmap, layout, closed: false })
    }

    pub(crate) fn add(&mut self, hash: &[u8], value: u64) {
        assert!(!self.closed, "index already closed");
        let index = read_hash_indices(&self.layout, hash)
            .skip_while(|index| self.layout.is_used(&self.mmap, *index))
            .next().unwrap();
        self.layout.set_entry(&mut self.mmap, index, hash, value);
    }

    pub(crate) fn finish(&mut self) {
        self.closed = true;
        Layout::set_preamble(&mut self.mmap)
    }
}

struct Layout {
    capacity: usize,
    hash1_offset: usize,
    hash2_offset: usize,
    hash3_offset: usize,
    value_offset: usize,
    min_file_size: usize,
}

impl Layout {
    /// Creates a layout for the given amount of items.
    fn from_item_count(item_count: usize) -> Layout {
        Layout::from_capacity(item_count + item_count / 2)
    }

    /// Creates a layout from the given content of a file.
    fn from_file(mmap: &[u8]) -> io::Result<Layout> {
        Layout::check_preamble(&mmap)?;
        let capacity = Layout::get_capacity(&mmap);
        let layout = Layout::from_capacity(capacity);
        if mmap.len() < layout.min_file_size {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "index file got truncated"))
        } else {
            Ok(layout)
        }
    }

    /// Creates the layout with the given capacity.
    /// Note that the index file is used as a hash map, so it should contain extra space.
    /// Use [Layout::from_item_count] to create the layout based on the amount of items you want to add.
    fn from_capacity(capacity: usize) -> Layout {
        assert_eq!(U64_SIZE, CAPACITY_OFFSET - BLOCK_SIZE_OFFSET);
        assert_eq!(U64_SIZE, BITSET_OFFSET - CAPACITY_OFFSET);
        assert_eq!(64, BITSET_OFFSET); // Verify expected offset as alignment on 8 bytes is important.
        let hash1_offset = BITSET_OFFSET + capacity.div_ceil(u8::BITS as usize).next_multiple_of(U64_SIZE);
        let hash2_offset = hash1_offset + (U64_SIZE * capacity);
        let hash3_offset = hash2_offset + (U64_SIZE * capacity);
        let value_offset = hash3_offset + (U64_SIZE * capacity);
        let min_file_size = value_offset + (U64_SIZE * capacity);
        Layout { capacity, hash1_offset, hash2_offset, hash3_offset, value_offset, min_file_size }
    }

    fn check_preamble(mmap: &[u8]) -> io::Result<()> {
        const P: &[u8] = PREAMBLE;
        match &mmap[..PREAMBLE_SIZE] {
            P => Ok(()),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "index file is invalid"))
        }
    }

    fn set_preamble(mmap: &mut MmapMut) {
        mmap[..PREAMBLE_SIZE].copy_from_slice(PREAMBLE);
    }

    fn get_block_size(mmap: &[u8]) -> u64 {
        let bytes: [u8; U64_SIZE] = mmap[BLOCK_SIZE_OFFSET..][..U64_SIZE].try_into().unwrap();
        u64::from_le_bytes(bytes)
    }

    fn set_block_size(mmap: &mut MmapMut, block_size: usize) {
        let value = block_size as u64;
        mmap[BLOCK_SIZE_OFFSET..][..U64_SIZE].copy_from_slice(&value.to_le_bytes());
    }

    fn get_capacity(mmap: &[u8]) -> usize {
        let bytes: [u8; U64_SIZE] = mmap[CAPACITY_OFFSET..][..U64_SIZE].try_into().unwrap();
        u64::from_le_bytes(bytes) as usize
    }

    fn set_capacity(&self, mmap: &mut MmapMut) {
        let value = self.capacity as u64;
        mmap[CAPACITY_OFFSET..][..U64_SIZE].copy_from_slice(&value.to_le_bytes());
    }

    fn is_used(&self, mmap: &[u8], index: usize) -> bool {
        let byte_offset = BITSET_OFFSET + index / u8::BITS as usize;
        let bitmask = 0b1000_0000 >> (index % u8::BITS as usize);
        (mmap[byte_offset] & bitmask) != 0
    }

    fn set_used(&self, mmap: &mut MmapMut, index: usize) {
        let byte_offset = BITSET_OFFSET + index / u8::BITS as usize;
        let bitmask = 0b1000_0000 >> (index % u8::BITS as usize);
        mmap[byte_offset] |= bitmask;
    }

    fn set_entry(&self, mmap: &mut MmapMut, index: usize, mut hash: &[u8], value: u64) {
        assert_eq!(false, self.is_used(mmap, index));
        let inner_offset = U64_SIZE * index;
        self.set_used(mmap, index);
        write_bytes(&mut hash, mmap[self.hash1_offset + inner_offset..][..U64_SIZE].as_mut());
        write_bytes(&mut hash, mmap[self.hash2_offset + inner_offset..][..U64_SIZE].as_mut());
        write_bytes(&mut hash, mmap[self.hash3_offset + inner_offset..][..U64_SIZE].as_mut());
        mmap[self.value_offset + inner_offset..][..U64_SIZE].copy_from_slice(&value.to_le_bytes());
    }

    fn get_hash1(&self, mmap: &[u8], index: usize) -> u64 {
        let offset = self.hash1_offset + U64_SIZE * index;
        u64::from_ne_bytes(mmap[offset..][..U64_SIZE].try_into().unwrap())
    }

    fn get_hash2(&self, mmap: &[u8], index: usize) -> u64 {
        let offset = self.hash2_offset + U64_SIZE * index;
        u64::from_ne_bytes(mmap[offset..][..U64_SIZE].try_into().unwrap())
    }

    fn get_hash3(&self, mmap: &[u8], index: usize) -> u64 {
        let offset = self.hash3_offset + U64_SIZE * index;
        u64::from_ne_bytes(mmap[offset..][..U64_SIZE].try_into().unwrap())
    }

    fn get_value(&self, mmap: &[u8], index: usize) -> u64 {
        let offset = self.value_offset + U64_SIZE * index;
        u64::from_le_bytes(mmap[offset..][..U64_SIZE].try_into().unwrap())
    }
}

fn read_hash_indices<'a>(layout: &'a Layout, hash: &[u8]) -> impl Iterator<Item=usize> + 'a {
    iter::successors(Some(read_hash_prefix(hash)), |prefix| Some(next_hash_prefix(*prefix)))
        .map(|prefix| (prefix % (layout.capacity as u128)) as usize)
}

fn read_hash_prefix(hash: &[u8]) -> u128 {
    u128::from_le_bytes(hash[..size_of::<u128>()].try_into().unwrap())
}

fn next_hash_prefix(previous: u128) -> u128 {
    previous.wrapping_mul(31)
}

fn write_bytes(src: &mut &[u8], dest: &mut [u8]) {
    let copied = src.read(dest).unwrap();
    dest[copied..].fill(0);
}
