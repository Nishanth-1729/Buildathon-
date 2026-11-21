#!/usr/bin/env python3
"""Drops a dataset's files from the page cache (no root needed), to time cold reads."""
import os
import sys

for root, _, files in os.walk(sys.argv[1]):
    for name in files:
        fd = os.open(os.path.join(root, name), os.O_RDONLY)
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        os.close(fd)
