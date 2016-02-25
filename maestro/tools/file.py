"""
Maestro File Tools
"""

import hashlib

def read_blocks(file, blocksize=1024):
    while True:
        block = file.read(blocksize)
        if block:
            yield block
        else:
            return

def md5_checksum(afile, blocksize=65536):
    with open(afile, "rb") as f:
        hasher = hashlib.md5()
        buf = f.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(blocksize)
        return hasher.hexdigest()
