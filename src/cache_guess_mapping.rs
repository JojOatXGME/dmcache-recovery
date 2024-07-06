use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::Path;

use bincode::Options;
use clap::{Parser, Subcommand};
use memmap2::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

mod index;

const HASH_BYTES: usize = 20;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct GlobalArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create index file from origin device
    Index(IndexArgs),
    /// Guess cache mappings
    Find(FindArgs),
}

#[derive(Parser, Debug)]
struct IndexArgs {
    /// Expected size of filesystem blocks in sectors (each 512 bytes, 8 KiB by default)
    #[arg(long, global = true, default_value_t = 16)]
    filesystem_block_size: usize,

    /// Size of cache blocks in sectors (each 512 bytes, 256 KiB by default)
    #[arg(long)]
    convert: bool,

    /// Size of a block in the index file in sectors (each 512 bytes, 8 KiB by default)
    #[arg(long, global = true, default_value_t = 16, requires("convert"))]
    index_block_size: usize,

    /// Path to the location where the index file shall be created
    #[arg()]
    index: Box<Path>,

    /// Path to the origin device or file
    #[arg()]
    origin: Box<Path>,
}

#[derive(Parser, Debug)]
struct FindArgs {
    /// Size of cache blocks in sectors (each 512 bytes, 256 KiB by default)
    #[arg(short = 's', long, default_value_t = 512)]
    cache_block_size: usize,

    /// Path to the index file
    #[arg()]
    index: Box<Path>,

    /// Path to the cache device or file
    #[arg()]
    cache: Box<Path>,
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    GlobalArgs::command().debug_assert();
}

fn main() {
    let args: GlobalArgs = GlobalArgs::parse();
    match &args.command {
        Commands::Index(cmd) => index(cmd),
        Commands::Find(cmd) => find(cmd),
    }
}

fn index(cmd: &IndexArgs) {
    let fs_block_size = 512 * cmd.filesystem_block_size;
    match cmd.convert {
        true => write_index_file(&cmd.index, fs_block_size,
                                 read_old_index(&cmd.origin, 512 * cmd.index_block_size)),
        false => write_index_file(&cmd.index, fs_block_size,
                                  read_origin_blocks(&cmd.origin, fs_block_size)),
    };
}

fn write_index_file(path: &Path, fs_block_size: usize, block_reader: impl ExactSizeIterator<Item=([u8; 20], u64)>) {
    let iteration_count = block_reader.len();
    let mut index_file = index::IndexBuilder::new(path, iteration_count, fs_block_size).unwrap();
    for (index, (hash, offset)) in block_reader.enumerate() {
        if index % 10240 == 0 {
            log_status(index, iteration_count, "blocks")
        }
        index_file.add(&hash, offset);
    }
    index_file.finish();
    log_complete(iteration_count, "blocks");
}

fn read_origin_blocks(path: &Path, fs_block_size: usize) -> impl ExactSizeIterator<Item=([u8; 20], u64)> {
    let (origin, origin_device_size) = open_file(path).unwrap();
    let origin_block_count = origin_device_size.div_ceil(fs_block_size);
    (0..origin_block_count).map(move |origin_block| {
        let origin_offset = origin_block * fs_block_size;
        let hash = hash_block(&origin, origin_block, fs_block_size);
        (hash, origin_offset as u64)
    })
}

fn read_old_index(path: &Path, index_block_size: usize) -> impl ExactSizeIterator<Item=([u8; 20], u64)> {
    #[derive(Serialize, Deserialize, Debug)]
    struct Entry {
        hash: [u8; HASH_BYTES],
        offset: u64,
    }

    let entry_size = serializer().serialized_size(&Entry { hash: [0; HASH_BYTES], offset: 0 }).unwrap() as usize;
    let entries_per_block = index_block_size / entry_size;
    assert_eq!(28, entry_size);

    let (index_file, index_size) = open_file(path).unwrap();
    let total_block_count = index_size / index_block_size;
    assert_eq!(0, index_size % index_block_size);

    let total_entry_count = total_block_count * entries_per_block;
    (0..total_entry_count).map(move |entry_index| {
        let block_index = entry_index / entries_per_block;
        let block_offset = block_index * index_block_size;
        let local_entry_index = entry_index % entries_per_block;
        let entry_offset = block_offset + local_entry_index * entry_size;
        let entry = serializer().deserialize::<Entry>(&index_file[entry_offset..entry_offset + entry_size]).unwrap();
        (entry.hash, entry.offset)
    })
}

fn find(cmd: &FindArgs) {
    let index = index::Index::open(&cmd.index).unwrap();

    let (cache_device, cache_device_size) = open_file(&cmd.cache).unwrap();
    let cache_block_size = 512 * cmd.cache_block_size;
    let cache_total_blocks = cache_device_size / cache_block_size;
    let fs_block_size = index.get_block_size();
    let fs_blocks_per_cache_block = cache_block_size / fs_block_size;

    for cache_block in 0..cache_total_blocks {
        log_status(cache_block, cache_total_blocks, "blocks\n");
        let mut matches = HashMap::new();
        let mut fake_matches = 0;

        for fs_block in 0..fs_blocks_per_cache_block {
            let hash = hash_block(&cache_device, cache_block * fs_blocks_per_cache_block + fs_block, fs_block_size);
            for match_offset in index.get(&hash) {
                let origin_fs_block = match_offset / fs_block_size;
                let origin_cache_block = match_offset / cache_block_size;
                let origin_local_fs_block = origin_fs_block % fs_blocks_per_cache_block;
                if origin_local_fs_block == fs_block {
                    *matches.entry(origin_cache_block).or_insert(0) += 1;
                } else {
                    fake_matches += 1;
                }
            }
        }

        let mut first = true;
        let mut match_vec: Vec<_> = matches.iter().collect();
        match_vec.sort_by_key(|&(_, count)| std::cmp::Reverse(count));
        for (origin_cache_block, count) in match_vec {
            println!(
                "{}{} -> {} ({:.2}% match)",
                if first { "" } else { "# " },
                cache_block,
                origin_cache_block,
                *count as f64 / fs_blocks_per_cache_block as f64 * 100.0,
            );
            first = false;
        }
        if first {
            println!("# no match found for cache block {}", cache_block)
        }
        if fake_matches != 0 {
            println!("# {} fake matches", fake_matches);
        }
    }
    log_complete(cache_total_blocks, "blocks");
}

fn hash_block(mmap: &Mmap, block: usize, block_size: usize) -> [u8; HASH_BYTES] {
    let offset = block_size * block;
    let data = &mmap[offset..offset + block_size];
    let mut hasher = Sha1::new();
    hasher.update(data);
    let result = hasher.finalize();
    assert_eq!(result.len(), HASH_BYTES);
    result.try_into().unwrap_or_else(|_| panic!("Cannot convert hash to array"))
}

fn serializer() -> impl Options {
    bincode::options().with_fixint_encoding()
}

fn open_file(path: &Path) -> io::Result<(Mmap, usize)> {
    let file = File::open(path)?;
    //let size = file.metadata()?.len();
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let size = mmap.len();
    return Ok((mmap, size));
}

fn log_status(current: usize, total: usize, unit: &str) {
    let percentage = 100.0 * (current as f64 / total as f64);
    eprint!(
        "{:5.1} % - {:} of {:} {}\r",
        percentage,
        current,
        total,
        unit,
    );
}

fn log_complete(total: usize, unit: &str) {
    eprintln!("100.0 % - {:} of {:} {}", total, total, unit);
}
