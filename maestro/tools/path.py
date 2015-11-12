import os

def get_tree_size(path = '.'):
    """
    get_tree_size will return the total size of a directory tree
    """
    if not os.path.exists(path):
        raise OSError("Path " + str(path) + " does not exist!")

    total_size = 0
    for dirpath, dirnames, filenames in os.walk(str(path)):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def get_case_insensitive_path(path = '.'):
    """
    get_case_insensitive_path will check for the existance of a path in a case sensitive file system, regardless of the case of the inputted path. Returns the absolute path if found (with correct casing) or None.
    """

    path_elements = full_split(path)
    path_root = None

    drive, path = os.path.splitdrive(path)
    if not drive:
        if not path.startswith("/"):
            path_root = os.path.abspath(os.path.normpath("./"))
        else:
            path_root = os.path.abspath(os.path.normpath("/"))
    else:
        path_root = os.path.abspath(os.path.normpath(drive))

    if not os.path.exists(path_root):
        raise OSError("Unable to locate path root: " + str(path_root))

    #Build the full path, also used for error messages 
    full_path = path_root
    for element in path_elements:
        if not element or element == "/":
            continue
        found = False
        for dir in next(os.walk(full_path))[1]:
            if element.lower() == dir.lower():
                full_path = os.path.join(full_path,dir)
                found = True
                break
        if found is False:
            raise OSError("The following path cannot be located: " + str(os.path.join(full_path,element)))

    return full_path


# Credit: John Machin
#         http://stackoverflow.com/questions/4579908/cross-platform-splitting-of-path-in-python
def full_split(path, debug=False):
    """
    full_split will split Windows and UNIX paths into seperate elements 
    """
    parts = []
    while True:
        newpath, tail = os.path.split(path)
        if debug: print repr(path), (newpath, tail)
        if newpath == path:
            assert not tail
            if path: parts.append(path)
            break
        parts.append(tail)
        path = newpath
    parts.reverse()
    return parts
