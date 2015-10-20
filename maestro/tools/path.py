import os

def get_tree_size(path = '.'):
    if not os.path.exists(path):
        raise OSError("Path " + str(path) + " does not exist!")

    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size
