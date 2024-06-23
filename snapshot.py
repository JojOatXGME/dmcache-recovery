#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Creates a snapshot of a device or file.

With this script you can easily create snapshot devices [1]_.
A snapshot is like a virtual copy of another device or file.
It allows you to make modifications without modifying the original.
Modifications are stored in a separate copy-on-write file. ::

    $ ./snapshot.py create snap /dev/sda1 sda1-cow.bin
    $ fsck /dev/mapper/snap
    $ ./snapshot.py unload snap

All the changes are preserved in ``sda1-cow.bin``.
You can later load the same snapshot again. ::

    $ ./snapshot.py load snap /dev/sda1 sda1-cow.bin

.. [1] Documentation of the ``snapshot`` target of device-mapper:
   https://www.kernel.org/doc/Documentation/device-mapper/snapshot.txt
"""

import os, re, shlex, subprocess, sys
from argparse import ArgumentParser
from contextlib import contextmanager
from pathlib import Path


def main():
    argparser = ArgumentParser()
    subparsers = argparser.add_subparsers(required=True)

    subparser = subparsers.add_parser("create")
    subparser.add_argument("name")
    subparser.add_argument("base", type=Path)
    subparser.add_argument("cow_device", type=Path)
    subparser.add_argument("--cow-device-size", type=int, default=2097152,
            help="Size of Copy-on-Write device in sectors")
    subparser.add_argument("--chunksize", type=int, default=512,
            help="Chunksize in sectors (512 bytes per sector)")
    #subparser.add_argument("--offset", help="Offset in base counted in bytes", type=int)
    #subparser.add_argument("--size", help="Offset in base counted in bytes", type=int)
    subparser.set_defaults(func=create)

    subparser = subparsers.add_parser("load")
    subparser.add_argument("name")
    subparser.add_argument("base", type=Path)
    subparser.add_argument("cow_device", type=Path)
    subparser.add_argument("--chunksize", type=int, default=512,
            help="Chunksize in sectors (512 bytes per sector)")
    subparser.set_defaults(func=load)

    subparser = subparsers.add_parser("unload")
    subparser.add_argument("name")
    subparser.set_defaults(func=unload)

    subparser = subparsers.add_parser("resize")
    subparser.add_argument("cow_file", type=Path)
    subparser.add_argument("new_size", type=int)
    subparser.set_defaults(func=resize)

    args = argparser.parse_args()
    args.func(args)


def create(args):
    with make_block_device(args.base) as base_device, \
            create_cow_device(args) as cow_device:
        setup_snapshot(args.name, base_device, cow_device, args.chunksize)


def load(args):
    with make_block_device(args.base) as base_device, \
            make_block_device(args.cow_device, write=True) as cow_device:
        setup_snapshot(args.name, base_device, cow_device, args.chunksize)


def setup_snapshot(name, base_device, cow_device, chunksize):
    size = detect_size(base_device)
    run(
        "dmsetup",
        "create",
        name,
        "--table",
        f"0 {size} snapshot {base_device} {cow_device} PO {chunksize}",
    )
    path = Path("/dev/mapper", name)
    if path.is_block_device():
        log_check(f"Device can be found at {path}")
    else:
        log_error(f"No device found at {path}")


def create_cow_device(args):
    size = 512 * args.cow_device_size
    cow_device = args.cow_device
    fd = None
    try:
        fd = os.open(cow_device, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.lseek(fd, size - 1, os.SEEK_SET)
        os.write(fd, b"\0")
        os.close(fd)
    except FileExistsError:
        log_error(f"{cow_device} already exists")
        sys.exit(1)
    except OSError as e:
        if fd is not None:
            try:
                os.close(fd)
            except:
                pass
            try:
                os.remove(cow_device)
            except:
                pass
        log_error(f"Failed to create copy-on-write file: {e.strerror}")
        sys.exit(1)

    log_check(f"Created copy-on-write file at {cow_device}")
    return make_block_device(cow_device, write=True)


def unload(args):
    path = Path("/dev/mapper", args.name)
    if not path.is_block_device():
        log_error(f"{path} not found or is not a block device")
        sys.exit(1)
    table = run_get_stdout("dmsetup", "table", str(path))
    match = re.fullmatch(r"""
      \s* 0               # logical_start_sector
      \s+ \d+             # num_sectors
      \s+ snapshot        # target_type
      \s+ (?P<origin>\S+) # origin
      \s+ (?P<cowdev>\S+) # COW device
      \s+ PO              # persistent
      \s+ \d+             # chunksize
      \s*
    """, table, re.ASCII | re.VERBOSE)
    if match is None:
        log_error(f"{path} is not a snapshot")
        sys.exit(1)
    origin = resolve_device_display_name(match.group("origin"))
    cowdev = resolve_device_display_name(match.group("cowdev"))
    log_check(f"{path} is a snapshot of {origin} using {cowdev}")

    run("dmsetup", "remove", "--retry", str(path))
    log_check(f"{path} has been removed")


def resolve_device_display_name(device):
    major_minor_match = re.fullmatch(r"(\d+):(\d+)", device)
    if major_minor_match is not None:
        #major = int(major_minor_match.group(1))
        #minor = int(major_minor_match.group(2))
        #device_number = os.makedev(major, minor)
        backing_file_path = Path(f"/sys/dev/block/{device}/loop/backing_file")
        if backing_file_path.exists():
            return Path(read_file(backing_file_path))
        return device
    log_error(f"Could not resolve device: {device}")
    sys.exit(1)


def resize(args):
    new_size = 512 * args.new_size
    cow_file = args.cow_file

    fd = None
    try:
        fd = os.open(cow_file, os.O_WRONLY)
        old_size = os.lseek(fd, 0, os.SEEK_END)
        log_check(f"{cow_file} has a size of {old_size} bytes")
        if new_size > old_size:
            os.lseek(fd, new_size - 1, os.SEEK_SET)
            os.write(fd, b"\0")
            os.close(fd)
            log_check(f"Changed size to {new_size} bytes")
        elif old_size == new_size:
            os.close(fd)
        else:
            log_error(f"Given size is smaller than the current size")
            sys.exit(1)
    except OSError as e:
        log_error(f"Could not update file size: {e.strerror}")
        sys.exit(1)

    # TODO Update capacity of all loop devices
    #run("losetup --associated file -o offset")
    #run("losetup", "--set-capacity", str(cow_device))


@contextmanager
def make_block_device(file_path, *, write=False):
    if file_path.is_block_device():
        yield file_path
        return

    stdout = run_get_stdout(
        "losetup",
        *([] if write else ["--read-only"]),
        "--find", "--nooverlap", "--show",
        #"--offset", "bytes",
        #"--sizelimit", "bytes",
        str(file_path),
        stdout=subprocess.PIPE,
        text=True,
    )

    result = Path(stdout)
    log_check(f"Created loop device {result} for {file_path}")
    yield result
    run("losetup", "--detach", str(result))
    log_check(f"Enabled autoclear of loop device {result}")


def detect_size(block_device):
    size = int(run_get_stdout("blockdev", "--getsz", str(block_device)))
    log_check(f"{block_device} has a size of {size} sectors")
    return size


def read_file(path):
    with open(path, "r") as file:
        return file.read().rstrip("\r\n")


def run_get_stdout(*command, **kwargs):
    return run(
        *command,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout.rstrip("\r\n")


def run(*command, **kwargs):
    print(f"> {shlex.join(command)}", file=sys.stderr)
    result = subprocess.run(command, **kwargs)
    if result.returncode == 0:
        log_check(f"{shlex.quote(command[0])} finished with exit code 0")
    else:
        log_error(f"{shlex.quote(command[0])} failed with exit code {result.returncode}")
        sys.exit(result.returncode)
    return result


def log_check(text):
    print(f"\u2713 {text}", file=sys.stderr)


def log_error(text):
    print(f"\u2717 {text}", file=sys.stderr)


if __name__ == "__main__":
    main()
