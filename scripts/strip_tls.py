#!/usr/bin/env python3
"""Zero a PE's TLS data directory.

Reflective injection maps the image and jumps straight to its entry point, so
the loader never runs TLS callbacks.  Any DLL that relies on them (typically a
statically linked MSVC CRT with thread-local state) faults on first use.  The
loader in native/ReflectiveLoader.c does not process TLS, so the directory has
to go.

Usage: strip_tls.py <src.dll> [dst.dll]

Idempotent: if the directory is already zero the file is left untouched, which
is the normal case for the /MT builds this repo produces.
"""
import shutil
import sys

import pefile


def main():
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print(__doc__)
        return 2

    src = sys.argv[1]
    dst = sys.argv[2] if len(sys.argv) == 3 else src

    if src != dst:
        shutil.copyfile(src, dst)

    # Read it fully, then close before writing back: pefile keeps a mapped view
    # of the file it parsed, and reopening the same path for writing while that
    # view is alive fails on Windows.
    pe = pefile.PE(dst)
    tls = pe.OPTIONAL_HEADER.DATA_DIRECTORY[9]
    rva, size = tls.VirtualAddress, tls.Size

    if rva == 0 and size == 0:
        print(f"strip_tls: {dst}: no TLS directory, nothing to do")
        return 0

    print(f"strip_tls: {dst}: TLS RVA=0x{rva:x} Size=0x{size:x} -> 0")
    tls.VirtualAddress = 0
    tls.Size = 0
    data = pe.write()
    pe.close()

    with open(dst, "wb") as fh:
        fh.write(data)
    return 0


if __name__ == "__main__":
    sys.exit(main())
