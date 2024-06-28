#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prints all blocks which don't match between the cache and the origin device.
For valid metadata, this script should not return any blocks.
When using ``--all``, only blocks marked as dirty should be printed. ::

    $ cache_dump -o metadata.xml /dev/mapper/cachemeta
    $ ./cache_verify.py metadata.xml /dev/mapper/cache /dev/mapper/data

This script also supports writing back the data from the cache into the origin device. ::

    $ cache_dump -o metadata.xml /dev/mapper/cachemeta
    $ ./cache_verify.py --writeback metadata.xml /dev/mapper/cache /dev/mapper/data

The previous commands write back dirty blocks and should be equivalent to the following command::

    cache_writeback --metadata-device /dev/mapper/cachemeta --origin-device /dev/mapper/data --fast-device /dev/mapper/cache

When using ``--writeback`` in combination with ``--all``,
the script writes back all data from the cache to the origin device,
ignoring whether the block is dirty or not.

The option ``--emulate`` allows to create a virtual device
which contains the data of the origin device as after using ``--writeback``.
This option uses the ``linear`` target [1]_ of device mapper. ::

    $ cache_dump -o metadata.xml /dev/mapper/cachemeta
    $ ./cache_verify.py --emulate writeback metadata.xml /dev/mapper/cache /dev/mapper/data
    $ ls /dev/mapper/writeback

.. [1] Documentation of the ``linear`` target of device-mapper:
   https://www.kernel.org/doc/Documentation/device-mapper/linear.txt
"""

import heapq, os, stat, sys
from argparse import ArgumentParser
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from subprocess import Popen, PIPE
from xml.sax import parse
from xml.sax.handler import ContentHandler


def main():
    argparser = ArgumentParser()
    argparser.add_argument("metadata", type=Path)
    argparser.add_argument("cache", type=Path)
    argparser.add_argument("origin", type=Path)
    argparser.add_argument("--all", action="store_true")
    argparser.add_argument("--emulate", metavar="DEVICE_NAME")
    argparser.add_argument("--table", action="store_true")
    argparser.add_argument("--writeback", action="store_true")
    args = argparser.parse_args()

    if len(list(filter(bool, [args.emulate, args.table, args.writeback]))) > 1:
        sys.exit("The options --emulate, --table and --writeback cannot used together")

    if args.emulate:
        emulate(args)
    elif args.table:
        print_table(args)
    elif args.writeback:
        writeback(args)
    else:
        verify(args)


def verify(args):
    with dev_open(args.cache) as fd_cache, dev_open(args.origin) as fd_origin:
        seen_targets = set()
        def callback(entry):
            cache_block = entry.cache_block
            origin_block = entry.origin_block
            block_size = entry.block_bytes
            print(f"{cache_block} -> {origin_block} (dirty={entry.dirty})", file=sys.stderr)

            if origin_block in seen_targets:
                print(f"cache block {cache_block} points to already seen origin block {origin_block}")
            else:
                seen_targets.add(origin_block)

            if not args.all and entry.dirty:
                return

            bytes_cache = dev_read_block(fd_cache, cache_block, block_size)
            if len(bytes_cache) != block_size:
                sys.exit(f"Incomplete cache block: {cache_block}")
            bytes_origin = dev_read_block(fd_origin, origin_block, block_size)
            if len(bytes_origin) != block_size:
                sys.exit(f"Incomplete origin block: {origin_block}")

            if bytes_cache != bytes_origin:
                print(f"cache block {cache_block} does not match origin block {origin_block}")

        read_metadata(args, callback)


def emulate(args):
    name = args.emulate
    with Popen(["dmsetup", "create", "-r", name], stdin=PIPE, text=True) as p:
        for line in generate_table(args):
            p.stdin.write(line)
            p.stdin.write("\n")
        p.stdin.close()
        p.wait()
        if p.returncode != 0:
            sys.exit(f"dmsetup failed with exit code {p.returncode}")


def print_table(args):
    for line in generate_table(args):
        print(line)


def is_effectively_dirty(entry, fd_origin, fd_cache):
    origin_bytes = dev_read_block(fd_origin, entry.origin_block, entry.block_bytes)
    cache_bytes = dev_read_block(fd_cache, entry.cache_block, entry.block_bytes)
    return origin_bytes != cache_bytes


def generate_table(args):
    cache_device = args.cache
    origin_device = args.origin
    device_size = get_device_size(origin_device)

    with dev_open(cache_device) as fd_cache, dev_open(origin_device) as fd_origin:
        heap = []
        def callback(entry):
            if entry.origin_sector + entry.block_sectors > device_size:
                sys.exit(f"block out of range: {entry.origin_sector}; device size: {device_size}")
            if args.all and is_effectively_dirty(entry, fd_origin, fd_cache) or entry.dirty:
                heapq.heappush(heap, (entry.origin_block, entry))

        read_metadata(args, callback)

    next_sector = 0
    while heap:
        current_entry = heapq.heappop(heap)[1]
        current_offset = current_entry.origin_sector
        block_size = current_entry.block_sectors
        if next_sector < current_offset:
            count = current_offset - next_sector
            yield f"{next_sector} {count} linear {origin_device} {next_sector}"
        yield f"{current_offset} {block_size} linear {cache_device} {current_entry.cache_sector}"
        next_sector = current_offset + block_size
    if next_sector < device_size:
        count = device_size - next_sector
        yield f"{next_sector} {count} linear {origin_device} {next_sector}"


def get_device_size(device):
    """Returns size of the given device in sectors"""
    statinfo = os.stat(device)
    if stat.S_ISREG(statinfo.st_mode):
        return statinfo.st_size // 512
    elif stat.S_ISBLK(statinfo.st_mode):
        device_id = statinfo.st_rdev
        device_id_str = f"{os.major(device_id)}:{os.minor(device_id)}"
        size_file = Path(f"/sys/dev/block/{device_id_str}/size")
        return int(size_file.read_text().rstrip("\r\n"))
    else:
        sys.exit(f"Unexpected file type: {device}")


def writeback(args):
    with dev_open(args.cache) as fd_cache, dev_open(args.origin, write=True) as fd_origin:
        def callback(entry):
            if args.all or entry.dirty:
                #print(f"{entry.cache_block} -> {entry.origin_block} (dirty={entry.dirty})", file=sys.stderr)
                dev_copy_block(fd_cache, entry.cache_block, fd_origin, entry.origin_block, entry.block_bytes)
        read_metadata(args, callback)


def read_metadata(args, callback):
    reader = MetadataReader(callback)
    parse(args.metadata, reader)


@dataclass(frozen=True, kw_only=True, slots=True)
class Entry:
    origin_block: int
    origin_sector: int
    origin_offset: int
    cache_block: int
    cache_sector: int
    cache_offset: int
    dirty: bool
    block_bytes: int
    block_sectors: int


class MetadataReader(ContentHandler):

    def __init__(self, callback):
        super().__init__()
        self.__callback = callback
        self.__block_size = None

    def startElement(self, name, attrs):
        if name == "superblock":
            if self.__block_size is not None:
                sys.exit("Invalid metadata: second superblock")
            self.__block_size = int(attrs["block_size"])
        if self.__block_size is None:
            sys.exit("Invalid metadata: No superblock")
        if name == "mapping":
            block_size = self.__block_size
            origin_block = int(attrs["origin_block"])
            cache_block = int(attrs["cache_block"])
            self.__callback(Entry(
                origin_block=origin_block,
                origin_sector=block_size * origin_block,
                origin_offset=512 * block_size * origin_block,
                cache_block=cache_block,
                cache_sector=block_size * cache_block,
                cache_offset=512 * block_size * cache_block,
                dirty=parse_bool(attrs["dirty"]),
                block_bytes=512 * block_size,
                block_sectors=block_size,
            ))


@contextmanager
def dev_open(path, *, write=False):
    fd = os.open(path, os.O_WRONLY if write else os.O_RDONLY)
    try:
        yield fd
    finally:
        os.close(fd)


def dev_read_block(fd, block, block_size):
    return os.pread(fd, block_size, block * block_size)


def dev_copy_block(src_fd, src_block, dest_fd, dest_block, block_size):
    os.lseek(src_fd, src_block * block_size, os.SEEK_SET)
    os.lseek(dest_fd, dest_block * block_size, os.SEEK_SET)
    return os.sendfile(dest_fd, src_fd, src_block * block_size, block_size)


def parse_bool(string):
    if string == "true":
        return True
    elif string == "false":
        return False
    else:
        raise ValueError("invalid boolean", string)


if __name__ == "__main__":
    main()
