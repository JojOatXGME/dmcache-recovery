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

    subparser = subparsers.add_parser("sort")
    subparser.add_argument("index")
    subparser.set_defaults(func=sort)

    subparser = subparsers.add_parser("lookup")
    subparser.add_argument("index")
    subparser.add_argument("cache_device")
    subparser.add_argument("block", required=False)
    subparser.set_defaults(func=lookup)

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

                block = device[offset:offset + BLOCK_SIZE]
                #assert len(block) == BLOCK_SIZE, f"len(block): {len(block)}; BLOCK_SIZE: {BLOCK_SIZE}"
                m = hashlib.sha1(usedforsecurity=False)
                m.update(block)
                digest = m.digest()
                assert len(digest) <= HASH_BYTES, f"len(digest): {len(digest)}; HASH_BYTES: {HASH_BYTES}"
                index_offset = index_block * BLOCK_SIZE + index_entry * ENTRY_SIZE
                index_file[index_offset:index_offset + ENTRY_SIZE] = struct.pack(STRUCT_FORMAT, digest, offset)
                index_entry += 1
                if index_entry >= ENTRIES_PER_BLOCK:
                    index_block += 1
                    index_entry = 0
            log_complete(device_size, "bytes")


def sort(args):
    with mmap_open(args.index, write=True) as mmap:
        heap = Heap(mmap)
        total_blocks = heap.block_count
        def status_callback(sorted_blocks):
            if sorted_blocks % 10 == 0:
                log_status(sorted_blocks, total_blocks, "blocks")
        heap.sort(status_callback):
        log_complete(total_blocks, "blocks")


def lookup(args):
    with mmap_open(args.index) as mmap:
        heap = Heap(mmap)
        pass


def log_status(current, total, unit):
    percentage = 100 * (current / total)
    print(f"{percentage:5.1f} % - {current:,d} of {total:,d} {unit}\r", end="", file=sys.stderr)


def log_complete(total, unit):
    print(f"100.0 % - {total:,d} of {total:,d} {unit}\r", end="", file=sys.stderr)


class Heap:

    def __init__(self, mmap):
        self.__mmap = mmap
        self.__block_count = mmap.size() // BLOCK_SIZE
        assert mmap.size() % BLOCK_SIZE == 0, f"mmap.size(): {mmap.size()}; BLOCK_SIZE: {BLOCK_SIZE}"

    @property
    def block_count(self):
        return self.__block_count

    def __length_hint__(self):
        return ENTRIES_PER_BLOCK * self.__block_count

    def __iter__(self):
        return self.__iter()

    def __iter(self, block_index=0):
        if block_index >= self.__block_count:
            return
        for i in range(ENTRIES_PER_BLOCK):
            yield from self.__iter(next_block(block_index, i))
            yield self.__get(block_index, i)
        yield from self.__iter(next_block(block_index, ENTRIES_PER_BLOCK))

    def __iter_own(self, block_index):
        assert block_index < self.__block_count, f"{block_index} < {self.__block_count}"
        for i in range(ENTRIES_PER_BLOCK):
            yield self.__get(block_index, i)

    def __get(self, block_index, entry):
        pass

    def find(self, needle, block_index=0):
        if block_index >= self.__block_count:
            return
        block_offset = block_index * BLOCK_SIZE
        for i in range(ENTRIES_PER_BLOCK):
            offset = block_offset + i * ENTRY_SIZE
            key, value = struct.unpack_from(STRUCT_FORMAT, buffer=self.__mmap, offset=offset)
            if key > needle:
                return find(needle, next_block(block_index, i))
            elif key == needle:
                return value
        return find(needle, next_block(block_index, ENTRIES_PER_BLOCK))

    def sort(callback, block_index=0, start=0, end=ENTRIES_PER_BLOCK):
        if block_index >= self.__block_count:
            return 0
        if start + 1 == end:
            return 0

        if start + 2 == end:
            ls

        # TODO Sort own
        # TODO yield
        # Sort all children
        sorted_blocks = 1
        for i in range(ENTRIES_PER_BLOCK + 1):
            child = next_block(block_index, i)
            sorted_blocks += self.sort(callback, child)
        # Merge
        for i in range(ENTRIES_PER_BLOCK):
            right = [self.__iter(next_block(block_index, j + 1)) for j in range(i, ENTRIES_PER_BLOCK)]
            smallest_it = chain_sorted(self.__iter_own(block_index), *right)
            smallest = next(smallest_it)
            for target in self.__iter(next_block(block_index, i)):
                if smallest < target:
                    self.__swap(target, smallest)
                    smallest = next(smallest_it)
            own = self.__get(block_index, i)
            if smallest < own:
                self.__swap(own, smallest)
        # Report number of sorted blocks
        callback(sorted_blocks)
        return sorted_blocks

    def __swap(entry1, entry2):
        pass # TODO

    def next_block(block_index, i):
        return (ENTRIES_PER_BLOCK + 1) * block_index + i + 1


#def chain_sorted(*iterables):
#    min_heap = []
#    for it in iterables:
#        heapq.heappush(min_heap, (next(it), it))
#    while min_heap:
#        smallest = min_heap[0]
#        yield smallest[0]
#        try:
#            heapq.heapreplace(min_heap, next(smallest[1]))
#        except StopIteration:
#            heapq.heappop(min_heap)


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
