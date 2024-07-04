use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use bincode::Options;
use clap::{Parser, Subcommand};
use memmap2::{Mmap, MmapMut, MmapOptions};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

const HASH_BYTES: usize = 20;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct GlobalArgs {
    /// Expected size of filesystem blocks in sectors (each 512 bytes, 8 KiB by default)
    #[arg(long, global = true, default_value_t = 16)]
    filesystem_block_bytes: usize,

    /// Size of a block in the index file in sectors (each 512 bytes, 8 KiB by default)
    #[arg(long, global = true, default_value_t = 16)]
    index_block_size: usize,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create index file from origin device
    Collect(CollectArgs),
    /// Guess cache mappings
    Find(FindArgs),
}

#[derive(Parser, Debug)]
struct CollectArgs {
    /// Path to the location where the index file shall be created
    #[arg()]
    index: Box<Path>,

    /// Path to the origin device or file
    #[arg()]
    origin: Box<Path>,
}

#[derive(Parser, Debug)]
struct FindArgs {
    /// Path to the index file
    #[arg()]
    index: Box<Path>,

    /// Path to the cache device or file
    #[arg()]
    cache: Box<Path>,

    /// Size of cache blocks in sectors (each 512 bytes, 256 KiB by default)
    #[arg(short = 's', long, default_value_t = 512)]
    cache_block_size: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct Entry {
    hash: [u8; HASH_BYTES],
    offset: u64,
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    GlobalArgs::command().debug_assert();
}

fn main() {
    let args: GlobalArgs = GlobalArgs::parse();
    match &args.command {
        Commands::Collect(cmd) => collect(&args, cmd),
        Commands::Find(cmd) => find(&args, cmd),
    }
}

fn collect(args: &GlobalArgs, cmd: &CollectArgs) {
    let entry_size = serializer().serialized_size(&Entry { hash: [0; HASH_BYTES], offset: 0 }).unwrap() as usize;
    let entries_per_block = 512 * args.index_block_size / entry_size;
    assert_eq!(entry_size, 28);

    let (origin, origin_device_size) = open_file(&cmd.origin).unwrap();
    let origin_block_count = div_rounding_up(origin_device_size, 512 * args.filesystem_block_bytes);
    let index_block_count = div_rounding_up(origin_block_count, entries_per_block);
    let mut index = create_file(&cmd.index, 512 * index_block_count * args.index_block_size).unwrap();

    let mut index_block: usize = 0;
    let mut index_entry: usize = 0;
    for origin_block in 0..origin_block_count {
        if origin_block % 10240 == 0 {
            log_status(origin_block, origin_block_count, "blocks")
        }
        let index_offset = index_block * 512 * args.index_block_size + index_entry * entry_size;
        index[index_offset..index_offset + entry_size].copy_from_slice(bincode::serialize(&Entry {
            hash: hash_block(&origin, origin_block, 512 * args.filesystem_block_bytes),
            offset: (origin_block * 512 * args.filesystem_block_bytes) as u64,
        }).unwrap().as_slice());

        index_entry += 1;
        if index_entry >= entries_per_block {
            index_block += 1;
            index_entry = 0;
        }
    }
    log_complete(origin_block_count, "blocks")
}

fn find(args: &GlobalArgs, cmd: &FindArgs) {
    let entry_size = serializer().serialized_size(&Entry { hash: [0; HASH_BYTES], offset: 0 }).unwrap() as usize;
    let entries_per_block = 512 * args.index_block_size / entry_size;
    assert_eq!(entry_size, 28);

    let index = {
        let mut index = HashMap::new();
        let (index_file, index_size) = open_file(&cmd.index).unwrap();
        let block_size = 512 * args.index_block_size;

        for block_offset in (0..index_size).step_by(block_size) {
            if block_offset % (1024 * block_size) == 0 {
                log_status(block_offset, index_size, "bytes");
            }
            for entry_index in 0..entries_per_block {
                let entry_offset = block_offset + entry_index * entry_size;
                let entry = serializer().deserialize::<Entry>(&index_file[entry_offset..entry_offset + entry_size]).unwrap();
                index.entry(entry.hash).or_insert_with(Vec::new).push(entry.offset as usize);
            }
        }
        log_complete(index_size, "bytes");

        index
    };

    let (cache_device, cache_device_size) = open_file(&cmd.cache).unwrap();
    let cache_block_size = 512 * cmd.cache_block_size;
    let cache_total_blocks = cache_device_size / cache_block_size;
    let fs_block_size = 512 * args.filesystem_block_bytes;
    let fs_blocks_per_cache_block = cache_block_size / fs_block_size;

    for cache_block in 0..cache_total_blocks {
        log_status(cache_block, cache_total_blocks, "blocks\n");
        let mut matches = HashMap::new();
        let mut fake_matches = 0;

        for fs_block in 0..fs_blocks_per_cache_block {
            let hash = hash_block(&cache_device, cache_block * fs_blocks_per_cache_block + fs_block, fs_block_size);
            if let Some(matches_vec) = index.get(&hash) {
                for match_offset in matches_vec {
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

fn create_file(path: &Path, size: usize) -> io::Result<MmapMut> {
    let file = OpenOptions::new().read(true).write(true).create_new(true).open(path)?;
    file.set_len(size as u64)?;
    let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
    Ok(mmap)
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

fn div_rounding_up(dividend: usize, divisor: usize) -> usize {
    (dividend + divisor - 1) / divisor
}
