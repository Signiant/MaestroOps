"""
Maestro File Tools
"""

def read_blocks(file, blocksize=1024):
    while True:
        block = file.read(blocksize)
        if block:
            yield block
        else:
            return
