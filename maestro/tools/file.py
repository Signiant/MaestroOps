"""
Maestro File Tools
"""

import hashlib
import base64

def read_blocks(file, blocksize=1024):
    while True:
        block = file.read(blocksize)
        if block:
            yield block
        else:
            return

def sha256_simple(filename):
    m = hashlib.sha256()
    with open(filename, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(8 * 1024 * 1024), b''):
            m.update(byte_block)
    return base64.b64encode(m.digest()).decode('utf-8')

def sha256_multipart(file_path, chunk_size=16 * 1024 * 1024):
    # chuck size of 16 mb works for our s3 downloads, multiplier may change if smaller chuck sizes
    sha256_hashes = []
    with open(file_path, 'rb') as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            sha256_hashes.append(hashlib.sha256(data))

    if len(sha256_hashes) == 1:
        return '"{}"'.format(sha256_hashes[0].hexdigest())

    digests = b''.join(sha.digest() for sha in sha256_hashes)
    digests_sha256 = hashlib.sha256(digests)
    return '{}-{}'.format(base64.b64encode(digests_sha256.digest()).decode('utf-8'), len(sha256_hashes))

def sha256_checksum(filename, checksum):
    checksum_stripped = checksum[1:-1]  # strip quotes
    if '-' in checksum_stripped:
        return sha256_multipart(filename)
    else:
        return sha256_simple(filename)

def md5_checksum(afile, blocksize=65536):
    with open(afile, "rb") as f:
        hasher = hashlib.md5()
        buf = f.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(blocksize)
        return hasher.hexdigest()
