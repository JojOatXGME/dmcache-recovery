#!/usr/bin/env python3

import hashlib, heapq, mmap, os, struct, sys
from argparse import ArgumentParser
from contextlib import contextmanager


HASH_BYTES = 20
BLOCK_SIZE = 8 * 1024
STRUCT_FORMAT = f"={HASH_BYTES}sQ"
ENTRY_SIZE = struct.calcsize(STRUCT_FORMAT)
ENTRIES_PER_BLOCK = BLOCK_SIZE // ENTRY_SIZE
MMAP_BLOCK_SIZE = 1024 * 1024 * 128


def main():
    argparser = ArgumentParser()
    #parser.add_argument("--filesystem-block-bytes", default=4096)
    #parser.add_argument("--index-block-bytes", default=4096)
    #parser.add_argument("--cache-block-bytes", default=262144)
    subparsers = argparser.add_subparsers(required=True)

    subparser = subparsers.add_parser("collect")
    subparser.add_argument("index")
    subparser.add_argument("device")
    subparser.set_defaults(func=collect)

    subparser = subparsers.add_parser("find")
    subparser.add_argument("index")
    subparser.add_argument("cache_device")
    #subparser.add_argument("block", required=False)
    subparser.add_argument("--cache-block-size", type=int, default=512, help="In sectors (512 bytes)")
    subparser.set_defaults(func=find)

    args = argparser.parse_args()
    args.func(args)


def collect(args):
    with mmap_open(args.device) as device:
        device_size = device.size()
        block_count = (device_size + BLOCK_SIZE - 1) // BLOCK_SIZE
        index_block_count = (block_count + ENTRIES_PER_BLOCK - 1) // ENTRIES_PER_BLOCK
        #print(f"device_size: {device_size}; block_count: {block_count}; index_block_count: {index_block_count}")
        with mmap_create(args.index, index_block_count * BLOCK_SIZE) as index_file:
            index_block = 0
            index_entry = 0
            for offset in range(0, device_size, BLOCK_SIZE):
                if offset % (BLOCK_SIZE * 10240) == 0:
                    log_status(offset, device_size, "bytes")

                digest = hash_block(device, offset, BLOCK_SIZE)
                assert len(digest) <= HASH_BYTES, f"len(digest): {len(digest)}; HASH_BYTES: {HASH_BYTES}"
                index_offset = index_block * BLOCK_SIZE + index_entry * ENTRY_SIZE
                index_file[index_offset:index_offset + ENTRY_SIZE] = struct.pack(STRUCT_FORMAT, digest, offset)
                index_entry += 1
                if index_entry >= ENTRIES_PER_BLOCK:
                    index_block += 1
                    index_entry = 0
            log_complete(device_size, "bytes")


def find(args):
    # Load index
    index = {}
    with mmap_open(args.index) as mmap:
        device_size = device.size()
        block_offset = 0
        while block_offset < device_size:
            if block_offset % (1024 * BLOCK_SIZE) == 0:
                log_status(block_offset, device_size, "bytes")
            for entry in range(ENTRIES_PER_BLOCK):
                index_offset = block_offset + entry * ENTRY_SIZE
                digest, offset = struct.unpack(STRUCT_FORMAT, mmap[index_offset:index_offset + ENTRY_SIZE])
                if offset % BLOCK_SIZE != 0:
                    sys.exit(f"Offset doesn't match fs block size: {offset}")
                index.setdefault(digest, []).append(offset)
            block_offset += BLOCK_SIZE
        log_complete(device_size, "bytes")

    # Read cache
    with mmap_open(args.cache_device) as mmap:
        cache_block_size = 512 * args.cache_block_size
        cache_total_blocks = mmap.size() // cache_block_size
        block_max_matches = cache_block_size // BLOCK_SIZE
        for cache_block in range(cache_total_blocks):
            log_status(cache_block, cache_total_blocks, "blocks", newline=True)
            matches = {}
            fake_matches = 0
            for fs_block in range(block_max_matches):
                digest = hash_block(mmap, cache_block * cache_block_size + fs_block * BLOCK_SIZE, BLOCK_SIZE)
                for match in index.get(digest, []):
                    origin_fs_block = match // BLOCK_SIZE
                    origin_cache_block = match // cache_block_size
                    origin_local_fs_block = origin_fs_block % block_max_matches
                    if origin_local_fs_block != fs_block:
                        fake_matches += 1
                        continue
                    matches.setdefault(origin_cache_block, 0)
                    matches[origin_cache_block] += 1
            first=True
            for match in sorted(matches.items(), key=lambda t: t[1], reverse=True):
                print(f"{'' if first else '#'}{cache_block} -> {match[0]} ({match[1] / block_max_matches:3%} match)")
                first=False
            if fake_matches != 0:
                print(f"#{fake_matches} fake matches")
        log_complete(cache_total_blocks, "blocks")


def hash_block(mmap, offset, size):
    block = mmap[offset:offset + size]
    # assert len(block) == size, f"len(block): {len(block)}; size: {size}"
    m = hashlib.sha1(usedforsecurity=False)
    m.update(block)
    return m.digest()


def log_status(current, total, unit, *, newline=False):
    percentage = 100 * (current / total)
    print(f"{percentage:5.1f} % - {current:,d} of {total:,d} {unit}", end="\n" if newline else "\r", file=sys.stderr)


def log_complete(total, unit):
    print(f"100.0 % - {total:,d} of {total:,d} {unit}\r", end="", file=sys.stderr)


@contextmanager
def mmap_create(path, size):
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_EXCL)
    try:
        os.lseek(fd, size - 1, os.SEEK_SET)
        os.write(fd, b"\0")
        with MMap(fd, size=size, access=mmap.ACCESS_WRITE) as m:
            yield m
    finally:
        os.close(fd)


@contextmanager
def mmap_open(path, write=False):
    fd = os.open(path, os if write else os.O_RDONLY)
    try:
        with MMap(fd, access=mmap.ACCESS_WRITE if write else mmap.ACCESS_READ) as m:
            yield m
    finally:
        os.close(fd)

class MMap:

    def __init__(self, fd, access, size=None):
        self.__fd = fd
        self.__access = access
        self.__size = os.lseek(fd, 0, os.SEEK_END) if size is None else size
        self.__offset = None
        self.__mmap = None

    def size(self):
        return self.__size

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        prev = self.__mmap
        self.__mmap = None
        self.__offset = None
        if prev is not None:
            prev.close()

    def __getitem__(self, key):
        if isinstance(key, slice):
            mmap, block_offset = self.mmap(key.start)
            new_start = key.start - block_offset
            new_stop = key.stop - block_offset
            assert new_stop <= MMAP_BLOCK_SIZE, f"new_stop: {new_stop}; MMAP_BLOCK_SIZE: {MMAP_BLOCK_SIZE}"
            return mmap[new_start:new_stop:key.step]
        else:
            raise TypeError("Invalid argument type")

    def __setitem__(self, key, value):
        if isinstance(key, slice):
            mmap, block_offset = self.mmap(key.start)
            new_start = key.start - block_offset
            new_stop = key.stop - block_offset
            assert new_stop <= MMAP_BLOCK_SIZE, f"new_stop: {new_stop}; MMAP_BLOCK_SIZE: {MMAP_BLOCK_SIZE}"
            mmap[new_start:new_stop:key.step] = value
        else:
            raise TypeError("Invalid argument type")

    def mmap(self, offset):
        block_offset = (offset // MMAP_BLOCK_SIZE) * MMAP_BLOCK_SIZE
        if self.__mmap is None or block_offset != self.__offset:
            if self.__mmap is not None:
                prev = self.__mmap
                self.__mmap = None
                prev.close()
            block_size = min(MMAP_BLOCK_SIZE, self.__size - block_offset)
            self.__mmap = mmap.mmap(self.__fd, block_size, access=self.__access, offset=block_offset)
        return self.__mmap, block_offset


if __name__ == "__main__":
    main()
